use bytes::BytesMut;
use httpdate::HttpDate;
use rocket::{
    http::{Header, HeaderMap},
    request::Request,
    response::{Responder, Response, Result}, 
    async_stream::try_stream, 
};
use s3::{request::DataStream, error::S3Error};
use serde::{
    Deserialize, 
    Serialize,
};
use serde_json::json;
use ssri::Integrity;
use std::{
    path::{PathBuf, Path}, 
    marker::PhantomData, 
    str::FromStr,
    time::SystemTime, 
};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use cacache::{
    WriteOpts, 
    Reader,
};


/// Cache mode
#[derive(Default, Debug, Deserialize, Serialize, PartialEq)]
pub enum CacheMode {
    Off,                // cache is inactive
    #[default]
    External,           // by NGINX or other caches...
    Internal            // by this module
}

/// Cache params
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct ConfigObjectCache {
    pub mode: CacheMode,
    pub root: Option<PathBuf>,
    pub max_age: Option<u64>,
    pub max_size: Option<u64>,
    pub inactive: Option<u64>,
    pub use_stale: Option<u64>,
    pub background_update: Option<u64>,
}

impl ConfigObjectCache {
    fn make_headers(&self) -> HeaderMap<'static> {
        let mut map = HeaderMap::new();
        // add cache-control header for external mode
        if self.mode == CacheMode::External {
            // default 10 sec fresh
            let max_age = self.max_age.unwrap_or(10); 
            // default not use stale resource
            let use_stale = self.use_stale.unwrap_or(0);
            let stale_directive = if use_stale > 0 {
                // use stale resource for 1 day
                format!(", stale-while-revalidate={use_stale}, stale-if-error={use_stale}")
            } else {
                // not use stale resource, must revalidate or return error
                String::from(", must-revalidate")
            };
            map.add(Header::new(
                "cache-control", 
                format!("max-age={max_age}{stale_directive}")
            ))
        }
        map
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct ObjectKey<'r> {
    bucket: &'r str,
    key: &'r str,
}

impl ObjectKey<'_> {
    fn cache_key(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}

impl<'a> AsRef<ObjectKey<'a>> for ObjectKey<'a> {
    fn as_ref(&self) -> &Self { 
        self
    }
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ObjectMeta {
    content_length: Option<usize>,
    content_type: Option<String>,
    last_modified: Option<SystemTime>,
    etag: Option<String>,
}

impl ObjectMeta {
    const CONTENT_LENGTH: &'static str = "content-length";    
    const CONTENT_TYPE: &'static str = "content-type";
    const LAST_MODIFIED: &'static str = "last-modified";
    const ETAG: &'static str = "etag";

    fn headers(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        if let Some(value) = self.content_length {
            map.add_raw(Self::CONTENT_LENGTH, value.to_string());
        }
        if let Some(ref value) = self.content_type {
            map.add_raw(Self::CONTENT_TYPE, value);
        }
        if let Some(value) = self.last_modified {
            let time = HttpDate::from(value);
            map.add_raw(Self::LAST_MODIFIED, time.to_string());
        }
        if let Some(ref value) = self.etag {
            map.add_raw(Self::ETAG, value);
        }
        map
    }
}

impl From<&HeaderMap<'_>> for ObjectMeta {
    fn from(headers: &HeaderMap) -> Self {
        // read content length header
        let content_length = headers.get_one(Self::CONTENT_LENGTH)
            .and_then(|s| s.parse::<usize>().ok());     
        // read content-type header
        let content_type = headers.get_one(Self::CONTENT_TYPE).map(String::from);
        // read & parse last-modified header
        let last_modified = headers.get_one(Self::LAST_MODIFIED)
            .and_then(|t| HttpDate::from_str(t).ok().map(|x| x.into()));
        // read etag header
        let etag = headers.get_one(Self::ETAG).map(String::from);

        Self {
            content_length,
            content_type,
            last_modified,
            etag,
        }
    } 
}

pub struct ObjectCache {
    config: ConfigObjectCache,
    headers: HeaderMap<'static>,
}

impl From<ConfigObjectCache> for ObjectCache {
    fn from(config: ConfigObjectCache) -> Self {
        let headers = config.make_headers();
        Self {
            config,
            headers,
        }
    }
}

fn save_stream<'a, P: AsRef<Path>, K: AsRef<ObjectKey<'a>>>(
    path: P,
    key: K,
    input: DataStream,
    meta: ObjectMeta
) -> DataStream {
    let cache = {
        let mut cache = PathBuf::new();
        cache.push(path);
        cache
    };
    let key = key.as_ref().cache_key();
    let stream = try_stream! {
        // create writer for store stream in cache
        let mut fd = {
            let opts = WriteOpts::new();
            // add size info to check integrity on commit
            if let Some(size) = meta.content_length  {
                opts.size(size)
            } else {
                opts
            }
            .metadata(json!{meta})
            .open(cache, key)
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?
        };

        // read values from input
        for await value in input {
            if let Ok(ref value) = value {
                // write value to cache file
                fd.write_all(value)
                    .await
                    .map_err(|e| S3Error::Io(e))?;
            }
            // yield to stream
            yield value?;
        }
        // check size and commit date in cache
        fd.commit()
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;
    };
    // return pinned stream
    Box::pin(stream)   
}


fn read_stream<P: AsRef<Path>>(
    path: P, 
    sri: Integrity
) -> DataStream {
    let cache = {
        let mut cache = PathBuf::new();
        cache.push(path);
        cache
    };
    let stream = try_stream! {
        // open cache for reading
        let mut fd = Reader::open_hash(cache, sri)
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;
        // read values from cache
        let mut buf = BytesMut::with_capacity(rocket::response::Body::DEFAULT_MAX_CHUNK);    
        while fd.read_buf(&mut buf).await? > 0 {
            let chunk = buf.split();
            yield chunk.freeze();
        }
        // check stream integrity
        fd.check().map_err(|e| S3Error::Http(500, e.to_string()))?;
    };
    // return pinned stream
    Box::pin(stream)
}

pub struct CacheResponder<'r, 'o: 'r, R: Responder<'r, 'o>> {
    inner: R,
    cache: &'r ObjectCache,
    #[doc(hidden)]
    _phantom: PhantomData<(&'r R, &'o R)>,
}

impl<'r, 'o: 'r, R: Responder<'r, 'o>> Responder<'r, 'o> for CacheResponder<'r, 'o, R> {
    fn respond_to(self, req: &'r Request<'_>) -> Result<'o> {
        let mut res = Response::build_from(self.inner.respond_to(req)?);
        for h in self.cache.headers.to_owned().into_iter() {
            res.header(h);
        }
        res.ok()
    }
}

impl <'r, 'o: 'r, R: Responder<'r, 'o>> CacheResponder<'r, 'o, R> {
    pub fn new(inner: R, cache: &'r ObjectCache) -> Self {
        Self {
            inner,
            cache,
            _phantom: PhantomData
        }
    }
}


#[cfg(test)]
mod test {
    use bytes::Bytes;
    use rocket::{
        http::ContentType, 
        async_stream::stream
    };
    use s3::request::StreamItem;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;
    use super::*;

    async fn pause() {
        let mut stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
    
        // we want the cursor to stay at the end of the line, so we print without a newline and flush manually.
        stdout.write_all(b"Press Enter to continue...").await.unwrap();
        stdout.flush().await.unwrap();
    
        // read a single byte and discard
        let _ = stdin.read(&mut [0u8]).await.unwrap();
    }

    #[tokio::test]
    async fn double_write_cache() {
        let dir = "./tests/my-cache1";
        let key = "my key";

        // write some data
        cacache::write(dir, key, b"my-async-data").await.unwrap();
        // write some data
        cacache::write(dir, key, b"my-async-data2").await.unwrap();

        // get the data back
        let data = cacache::read(dir, key).await.unwrap();
        assert_eq!(data, b"my-async-data2");

        // clean up the cache
        cacache::clear(dir).await.unwrap();
    }

    #[tokio::test]
    async fn cache_stream() {
        let dir = "./tests/my-cache2";
        let key = ObjectKey {
            bucket: "bucket1",
            key: "my key"
        };

        let meta1 = ObjectMeta { 
            content_type: Some(ContentType::HTML.to_string()), 
            content_length: Some(80),
            last_modified: Some(SystemTime::now()),
            etag: Some("some-etag".to_string()), 
        };

        // input stream
        let input = Box::pin(
            stream! {
                for i in (0..10) {
                    let item: StreamItem = Ok(Bytes::from(format!("hello {}\n", i)));
                    yield item
                }
            }
        );

        // save input to cache and return persisted stream
        let saved = save_stream(dir, key, input, meta1.clone());
        // read to buffer1
        let data1 = {
            let mut reader = StreamReader::new(saved);
            let mut data = vec![];
            reader.read_to_end(&mut data).await.unwrap();
            data
        };

        // uncomment to pause to manually edit the cache file and cause the integrity check to fail
        // press Enter in terminal to continue
        //pause().await;

        // get stream from cache
        let md = cacache::metadata(dir, key.cache_key()).await.unwrap().unwrap();
        let sri = md.integrity;
        let meta2: ObjectMeta = serde_json::from_value(md.metadata).unwrap();
        let cached = read_stream(dir, sri);
        // read to buffer2
        let data2 = {
            let mut reader = StreamReader::new(cached);
            let mut data = vec![];
            reader.read_to_end(&mut data).await.unwrap();
            data
        };

        // compare buffers
        assert_eq!(data1, data2);

        // compare metas
        assert_eq!(meta1, meta2);

        // clean up the cache
        cacache::clear(dir).await.unwrap();
    }

    #[tokio::test]
    async fn object_meta_headers() {
        let mut headers = HeaderMap::new();
        headers.add_raw("content-length", "12096836");
        headers.add_raw("content-type", "image/jpeg");
        headers.add_raw("last-modified", "Fri, 10 Feb 2023 09:57:33 GMT");
        headers.add_raw("etag", "76febaf5c48e1e2c834c6663d0cbedcb");

        let meta = ObjectMeta::from(&headers);

        assert_eq!(headers, meta.headers());
    }
}