use bytes::BytesMut;
use rocket::{
    http::Header,
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
};
use time::{
    format_description::well_known::Rfc2822,
    OffsetDateTime,
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

/// Cache response headers 
type Headers = Vec<Header<'static>>;

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
    fn make_headers(&self) -> Headers {
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
            vec![
                Header::new("cache-control", format!("max-age={max_age}{stale_directive}"))
            ]
        } else {
            vec![]
        }
    }
}


#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct ObjectMeta {
    bucket: String,
    key: String,
    last_modified: OffsetDateTime,
    etag: String,
    size: usize,
}

impl ObjectMeta {
    fn cache_key(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}


pub struct ObjectCache {
    config: ConfigObjectCache,
    headers: Headers,
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

fn save_stream<P: AsRef<Path>>(
    path: P, 
    input: DataStream,
    meta: ObjectMeta
) -> DataStream {
    let cache = {
        let mut cache = PathBuf::new();
        cache.push(path);
        cache
    };
    let stream = try_stream! {
        // create writer for store stream in cache
        let mut fd = WriteOpts::new()
            .size(meta.size)
            .metadata(json!{meta})
            .open(cache, meta.cache_key())
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;

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
        let mut buf = BytesMut::with_capacity(4096);    
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
        for h in self.cache.headers.iter() {
            res.header(h.to_owned());
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
    use rocket::{http::hyper::body::Bytes, async_stream::stream};
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
    
        // wead a single byte and discard
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
        let key = "my key";
        let bucket = "bucket1";

        let meta1 = ObjectMeta { 
            bucket: bucket.to_string(), 
            key: key.to_string(),
            last_modified: OffsetDateTime::now_utc(),
            etag: "some-etag".to_string(), 
            size: 80
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
        let saved = save_stream(dir, input, meta1.clone());
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
        let md = cacache::metadata(dir, meta1.cache_key()).await.unwrap().unwrap();
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
}