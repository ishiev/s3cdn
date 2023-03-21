use bytes::BytesMut;
use httpdate::HttpDate;
use rocket::{
    http::{Header, HeaderMap}, 
    async_stream::try_stream, 
};
use s3::{
    request::DataStream, 
    error::S3Error
};
use serde::{
    Deserialize, 
    Serialize,
};
use serde_json::json;
use ssri::Integrity;
use std::{
    path::{PathBuf, Path}, 
    str::FromStr,
    time::SystemTime, 
    collections::HashMap, 
    future::Future, 
    borrow::Cow, 
};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use cacache::{
    WriteOpts, 
    Reader,
};


/// Cache mode
#[derive(Default, Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
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

#[derive(Debug, Copy, Clone)]
pub struct ObjectKey<'r> {
    pub bucket: &'r str,
    pub key: &'r Path,
}

impl ObjectKey<'_> {
    fn cache_key(&self) -> String {
        format!("{}/{}", self.bucket, self.key.to_string_lossy())
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
    date: Option<SystemTime>,
    last_modified: Option<SystemTime>,
    etag: Option<String>,
}

impl ObjectMeta {
    const CONTENT_LENGTH: &'static str = "content-length";    
    const CONTENT_TYPE: &'static str = "content-type";
    const DATE: &'static str = "date";
    const LAST_MODIFIED: &'static str = "last-modified";
    const ETAG: &'static str = "etag";

    pub fn headers(&self) -> HeaderMap<'static> {
        let mut map = HeaderMap::new();
        if let Some(value) = self.content_length {
            map.add_raw(Self::CONTENT_LENGTH, value.to_string());
        }
        if let Some(ref value) = self.content_type {
            map.add_raw(Self::CONTENT_TYPE, value.to_string());
        }
        if let Some(value) = self.date {
            let time = HttpDate::from(value);
            map.add_raw(Self::DATE, time.to_string());
        }
        if let Some(value) = self.last_modified {
            let time = HttpDate::from(value);
            map.add_raw(Self::LAST_MODIFIED, time.to_string());
        }
        if let Some(ref value) = self.etag {
            map.add_raw(Self::ETAG, value.to_string());
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
        // read & parse date header
        let date = headers.get_one(Self::DATE)
            .and_then(|t| HttpDate::from_str(t).ok().map(|x| x.into()));
        // read & parse last-modified header
        let last_modified = headers.get_one(Self::LAST_MODIFIED)
            .and_then(|t| HttpDate::from_str(t).ok().map(|x| x.into()));
        // read etag header
        let etag = headers.get_one(Self::ETAG).map(String::from);

        Self {
            content_length,
            content_type,
            date,
            last_modified,
            etag,
        }
    } 
}

impl From<&HashMap<String, String>> for ObjectMeta {
    
    fn from(headers: &HashMap<String, String>) -> Self {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.add_raw(name, value)
        }
        Self::from(&map)
    }
}

// Values from conditional request
#[derive(Default, Debug)]
pub struct ConditionalHeaders<'r> {
    if_modified_since: Option<Cow<'r, str>>,
    if_none_match: Option<Cow<'r, str>>
}

impl<'r> ConditionalHeaders<'r> {
    const IF_MODIFIED_SINCE: &'static str = "if-modified-since";
    const IF_NONE_MATCH: &'static str = "if-none-match";

    pub fn headers(self) -> HeaderMap<'r> {
        let mut map = HeaderMap::new();
        if let Some(value) = self.if_modified_since {
            map.add_raw(Self::IF_MODIFIED_SINCE, value);
        }
        if let Some(value) = self.if_none_match {
            map.add_raw(Self::IF_NONE_MATCH, value);
        }
        map
    }

    pub fn is_empty(&self) -> bool {
        self.if_modified_since.is_none() && self.if_none_match.is_none()
    }
}

/// Construct from object meta
impl <'r> From<ObjectMeta> for ConditionalHeaders<'r> {
    fn from(value: ObjectMeta) -> Self {
        let if_modified_since = value.last_modified
            .map(|x| HttpDate::from(x).to_string().into());
        let if_none_match = value.etag
            .map(Cow::from);
        Self {
            if_modified_since,
            if_none_match
        }
    }
}

/// Construst from headers
impl <'r> From<&'r HeaderMap<'r>> for ConditionalHeaders<'r> {
    fn from(headers: &'r HeaderMap<'_>) -> Self {
        Self {
            if_modified_since: headers.get_one(Self::IF_MODIFIED_SINCE).map(Cow::from),
            if_none_match: headers.get_one(Self::IF_NONE_MATCH).map(Cow::from)
        }
    }
}

/// Cached data object 
pub struct DataObject {
    pub meta: ObjectMeta,
    pub stream: DataStream,
    pub status: Option<Header<'static>>
}

/// Variants for cache object revalidation
enum ValidationResult {
    Origin(DataObject),
    Fresh(DataObject),
    Revalidated(DataObject),
    Updated(DataObject),
    Stale(DataObject),
    NotFound(S3Error),
    Err(S3Error)
}

impl ValidationResult {
    const HEADER_NAME: &'static str = "x-s3cdn-status";
    const ORIGIN: &'static str = "Origin";
    const FRESH: &'static str = "Fresh";
    const REVALIDATED: &'static str = "Revalidated";
    const UPDATED: &'static str = "Updated";
    const STALE: &'static str = "Stale";
    const NOT_FOUND: &'static str = "NotFound";
    const ERROR: &'static str = "Error";

    /// Make x-s3cdn-status header from &self
    fn header(&self) -> Header<'static> {
        let value = match self {
            Self::Origin(_) => Self::ORIGIN,
            Self::Fresh(_) => Self::FRESH,
            Self::Revalidated(_) => Self::REVALIDATED,
            Self::Updated(_) => Self::UPDATED,
            Self::Stale(_) => Self::STALE,
            Self::NotFound(_) => Self::NOT_FOUND,
            Self::Err(_) => Self::ERROR,
        };
        Header::new(Self::HEADER_NAME, value)
    }

}

/// Object cache
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

impl Drop for ObjectCache {
    fn drop(&mut self) {
        info!("DROPPING CACHE!")
    }
}

impl ObjectCache {
    /// Get cache-control headers for response
    pub fn headers(&self) -> HeaderMap<'static> {
        self.headers.to_owned()
    }

    pub fn mode(&self) -> CacheMode {
        self.config.mode
    }

    /// Get object from cache or from origin source, bypass if mode != Internal
    pub async fn get_object<'a, K, F, Fut>(&self, key: K, origin: F, condition: ConditionalHeaders<'a>)
    -> Result<DataObject, S3Error> 
    where 
        K: AsRef<ObjectKey<'a>> + std::marker::Copy,
        F: FnOnce(ConditionalHeaders<'a>)->Fut,
        Fut: Future<Output = Result<DataObject, S3Error>>
    {
        if self.mode() == CacheMode::Internal {
            self.get_cached_object(key, origin, condition).await
        } else {
            origin(condition).await
        }
    }

    /// Get object from cache or from origin source
    pub async fn get_cached_object<'a, K, F, Fut>(&self, key: K, origin: F, condition: ConditionalHeaders<'a>) 
    -> Result<DataObject, S3Error> 
    where 
        K: AsRef<ObjectKey<'a>> + std::marker::Copy,
        F: FnOnce(ConditionalHeaders<'a>)->Fut,
        Fut: Future<Output = Result<DataObject, S3Error>>
    {
        let cache = self.config.root
            .as_ref()
            .ok_or(S3Error::Http(500, "Cache root config param not defined".to_string()))?;
        let key = key.as_ref().cache_key();
        
        // try to get object from cache
        let (obj, sri) = try_get_object(cache, &key)
            .await? 
            .map_or((None, None), |(obj, sri)| (Some(obj), Some(sri)));
        
        // validate cache object
        let res = self.validate(obj, origin).await;
        let status = res.header();
        
        // modify cache (if nessesary) and return result
        match res {
            ValidationResult::Origin(obj) => {
                // got new object from origin
                // save stream to cache
                let stream = save_stream(
                    cache, 
                    key, 
                    obj.stream, 
                    obj.meta.to_owned()
                );
                // return new object
                Ok(DataObject {
                    meta: obj.meta,
                    stream,
                    status: Some(status)
                })
            },
            ValidationResult::Fresh(mut obj) => {
                // fresh from cache 
                // return with status
                obj.status = Some(status);
                check_condition(obj, condition)
            },
            ValidationResult::Revalidated(mut obj) => {
                // cache object revalidated by origin
                // update time and status
                obj.meta.date = Some(SystemTime::now());
                obj.status = Some(status);  
                // make new metadata
                let sri = sri
                    .ok_or(S3Error::Http(500, "Error get sri while removing object from cache".to_string()))?;
                let opts = WriteOpts::new()
                    .integrity(sri)
                    .size(obj.meta.content_length.unwrap_or(0))
                    .metadata(json!(obj.meta));
                // insert metadata with updated time
                cacache::index::insert_async(cache, &key, opts)
                    .await
                    .map_err(|e| S3Error::Http(500,format!("Error insert new metadata: {e}")))?;
                // return from cache
                check_condition(obj, condition)
            },
            ValidationResult::Updated(obj) => {
                // got updated object from origin
                // replace object in cache
                let stream = save_stream(
                    cache, 
                    key, 
                    obj.stream, 
                    obj.meta.to_owned()
                );
                // return object from origin
                Ok(DataObject {
                    meta: obj.meta,
                    stream,
                    status: Some(status)
                })
            },
            ValidationResult::Stale(mut obj) => {
                // got stale object from cache
                // return with status
                obj.status = Some(status);
                check_condition(obj, condition)
            },
            ValidationResult::NotFound(e) => {
                // object not found in origin
                // remove object from cache
                let sri = sri
                    .ok_or(S3Error::Http(500, "Error get sri while removing object from cache".to_string()))?;
                cacache::remove_hash(cache, &sri)
                    .await
                    .map_err(|e| S3Error::Http(500,format!("Error remove object: {e}")))?;
                // return error
                Err(e)
            },
            ValidationResult::Err(e) => Err(e)
        }
    }

    /// Validate object or get new from origin
    async fn validate<'a, F, Fut>(&self, obj: Option<DataObject>, origin: F)
    -> ValidationResult 
    where
        F: FnOnce(ConditionalHeaders<'a>)->Fut,
        Fut: Future<Output = Result<DataObject, S3Error>>
    {
        if let Some(obj) = obj {         

            let time = obj.meta.date
                .unwrap_or(std::time::UNIX_EPOCH);
            let age = SystemTime::now()
                .duration_since(time)
                .unwrap_or_default()
                .as_secs();
            let max_age = self.config.max_age.unwrap_or_default();
            
            // check the object age
            if age > max_age{
                // object expired, need revalidation
                let conditional = ConditionalHeaders::from(obj.meta.clone());
                // request origin for revalidate
                match origin(conditional).await {
                    Ok(new_obj) => ValidationResult::Updated(new_obj),
                    Err(S3Error::Http(304, _)) => ValidationResult::Revalidated(obj),
                    Err(S3Error::Http(404, msg)) => ValidationResult::NotFound(
                        S3Error::Http(404, format!("Object was removed from origin, error message: {msg}"))
                    ),
                    Err(e) => {
                        // check for stale config
                        match self.config.use_stale {
                            Some(stale) if age < stale + max_age => {
                                // can return stale object from cache
                                ValidationResult::Stale(obj)
                            },
                            _ => ValidationResult::Err(e)
                        }   
                    }
                }
            } else {
                // object fresh, return back
                ValidationResult::Fresh(obj)
            }
        } else {
            // get object from origin source
            match origin(ConditionalHeaders::default()).await {
                Ok(new_obj) => ValidationResult::Origin(new_obj),
                Err(e) => ValidationResult::Err(e)
            }  
        }
    }
}

fn check_condition(obj: DataObject, condition: ConditionalHeaders) 
-> Result<DataObject, S3Error> {
    // check etag
    if let Some(req_etag) = condition.if_none_match {
        if let Some(ref obj_etag) = obj.meta.etag {
            if &req_etag == obj_etag {
                // not modified
                return Err(S3Error::Http(304, String::new()))
            } else {
                // modified
                return Ok(obj)
            }
        }
    } 
    // check modified time
    if let Some(req_modified_str) = condition.if_modified_since {
        if let Some(ref obj_modified) = obj.meta.last_modified {
            let req_modified = HttpDate::from_str(req_modified_str.as_ref())
                .ok()
                .map(|x| x.into())
                .unwrap_or(std::time::UNIX_EPOCH);
            if &req_modified == obj_modified {
                // not modified
                return Err(S3Error::Http(304, String::new()))
            }
        }
    }
    // modified
    Ok(obj)
}

async fn try_get_object<P: AsRef<Path>, K: AsRef<str>>(
    path: P,
    key: K
) -> Result<Option<(DataObject, Integrity)>, S3Error> {
    if let Some(md) = cacache::metadata(&path, key)
        .await
        .map_err(|e| S3Error::Http(500, e.to_string()))? {
        // check data in cache
        let sri = md.integrity;
        if cacache::exists(&path, &sri).await {
            // decode meta from json
            let meta: ObjectMeta = serde_json::from_value(md.metadata)
                .map_err(|e| S3Error::Http(500, e.to_string()))?;
            // get stream from cache and construct object
            let stream = read_stream(&path, sri.to_owned());
            Ok(Some(
                (DataObject {
                    meta,
                    stream,
                    status: None,
                },
                sri)
            ))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn save_stream<P: AsRef<Path>, K: AsRef<str>>(
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
    let key = String::from(key.as_ref());
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
    };
    // return pinned stream
    Box::pin(stream)
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

    #[allow(dead_code)]
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
            key: Path::new("my key")
        }.cache_key();

        let meta1 = ObjectMeta { 
            content_type: Some(ContentType::HTML.to_string()), 
            content_length: Some(80),
            date: Some(SystemTime::now()),
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
        let saved = save_stream(dir, &key, input, meta1.clone());
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
        let md = cacache::metadata(dir, &key).await.unwrap().unwrap();
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
        headers.add_raw("date", HttpDate::from(SystemTime::now()).to_string());
        headers.add_raw("last-modified", "Fri, 10 Feb 2023 09:57:33 GMT");
        headers.add_raw("etag", "76febaf5c48e1e2c834c6663d0cbedcb");

        let meta = ObjectMeta::from(&headers);

        assert_eq!(headers, meta.headers());
    }
}