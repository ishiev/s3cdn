use bytes::BytesMut;
use http::{header, HeaderMap, HeaderName};
use httpdate::HttpDate;
use s3::{
    request::DataStream, 
    error::S3Error, 
};
use serde::{
    Deserialize, 
    Serialize,
};
use serde_json::json;
use ssri::Integrity;
use std::{
    path::{PathBuf}, 
    str::FromStr,
    time::SystemTime,
    collections::HashMap, 
    future::Future, 
    sync::Arc 
};
use tokio::io::{
    AsyncWriteExt, 
    AsyncReadExt
};
use async_stream::try_stream;
use crate::{
    metacache::{MetaCache, Metadata},
    housekeeper::ConfigHousekeeper,
    error::Error,
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
    pub use_stale: Option<u64>,
    pub in_memory_items: Option<u64>,
}

impl ConfigObjectCache {
    fn make_headers(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        // add cache-control header for external mode
        if self.mode == CacheMode::External {
            // default 0 secs
            let max_age = self.max_age.unwrap_or_default(); 
            // default not use stale resource
            let use_stale = self.use_stale.unwrap_or_default();
            let stale_directive = if use_stale > 0 {
                // use stale resource
                format!(", stale-while-revalidate={use_stale}, stale-if-error={use_stale}")
            } else {
                // not use stale resource, must revalidate or return error
                String::from(", must-revalidate")
            };
            map.insert(
                header::CACHE_CONTROL, 
                format!("max-age={max_age}{stale_directive}").parse().unwrap()
            );
        }
        map
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObjectKey {
    pub bucket: String,
    pub key: String,
}

impl ObjectKey {
    pub fn cache_key(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}

impl AsRef<ObjectKey> for ObjectKey {
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
    pub fn headers(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        if let Some(value) = self.content_length {
            map.insert(header::CONTENT_LENGTH, value.into());
        }
        if let Some(ref value) = self.content_type {
            map.insert(header::CONTENT_TYPE, value.parse().unwrap());
        }
        if let Some(value) = self.date {
            let time = HttpDate::from(value);
            map.insert(header::DATE, time.to_string().parse().unwrap());
        }
        if let Some(value) = self.last_modified {
            let time = HttpDate::from(value);
            map.insert(header::LAST_MODIFIED, time.to_string().parse().unwrap());
        }
        if let Some(ref value) = self.etag {
            map.insert(header::ETAG, value.parse().unwrap());
        }
        map
    }
}

impl From<&HeaderMap> for ObjectMeta {
    fn from(headers: &HeaderMap) -> Self {
        // read content length header
        let content_length = headers.get(header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());     
        // read content-type header
        let content_type = headers.get(header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .map(String::from);
        // read & parse date header
        let date = headers.get(header::DATE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| HttpDate::from_str(s).ok())
            .map(|x| x.into());
        // read & parse last-modified header
        let last_modified = headers.get(header::LAST_MODIFIED)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| HttpDate::from_str(s).ok())
            .map(|x| x.into());
        // read etag header
        let etag = headers.get(header::ETAG)
            .and_then(|h| h.to_str().ok())
            .map(String::from);

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
            let Ok(key) = HeaderName::from_bytes(name.as_bytes())
            else { continue };
            let Ok(value) = value.parse()
            else { continue };
            map.insert(key, value);
        }
        Self::from(&map)
    }
}

// Values from conditional request
#[derive(Default, Debug)]
pub struct ConditionalHeaders {
    if_modified_since: Option<String>,
    if_none_match: Option<String>
}

impl ConditionalHeaders {
    pub fn headers(self) -> HeaderMap {
        let mut map = HeaderMap::new();
        if let Some(value) = self.if_modified_since {
            if let Ok(value) = value.parse() {
                map.insert(header::IF_MODIFIED_SINCE, value);
            }
        }
        if let Some(value) = self.if_none_match {
            if let Ok(value) = value.parse() {
                map.insert(header::IF_NONE_MATCH, value);
            }
        }
        map
    }
}

/// Construct from object meta
impl From<ObjectMeta> for ConditionalHeaders {
    fn from(value: ObjectMeta) -> Self {
        let if_modified_since = value.last_modified
            .map(|x| HttpDate::from(x).to_string());
        let if_none_match = value.etag;
        Self {
            if_modified_since,
            if_none_match
        }
    }
}

/// Construst from headers
impl From<&HeaderMap> for ConditionalHeaders {
    fn from(headers: &HeaderMap) -> Self {
        Self {
            if_modified_since: headers.get(header::IF_MODIFIED_SINCE)
                .and_then(|h| h.to_str().ok())
                .map(String::from),
            if_none_match: headers.get(header::IF_NONE_MATCH)
                .and_then(|h| h.to_str().ok())
                .map(String::from)
        }
    }
}

/// Cached data object 
pub struct DataObject {
    pub meta: ObjectMeta,
    pub stream: DataStream,
    pub status: Option<&'static str>
}

/// Variants for cache object revalidation
enum ValidationResult {
    Origin(DataObject),
    Fresh(DataObject),
    Revalidated(DataObject),
    Updated(DataObject),
    Stale(DataObject),
    NotFound(Error),
    Err(Error)
}

impl ValidationResult {
    const ORIGIN: &'static str = "Origin";
    const FRESH: &'static str = "Fresh";
    const REVALIDATED: &'static str = "Revalidated";
    const UPDATED: &'static str = "Updated";
    const STALE: &'static str = "Stale";
    const NOT_FOUND: &'static str = "NotFound";
    const ERROR: &'static str = "Error";

    /// Make x-s3cdn-status str from &self
    fn as_str(&self) -> &'static str {
        match self {
            Self::Origin(_) => Self::ORIGIN,
            Self::Fresh(_) => Self::FRESH,
            Self::Revalidated(_) => Self::REVALIDATED,
            Self::Updated(_) => Self::UPDATED,
            Self::Stale(_) => Self::STALE,
            Self::NotFound(_) => Self::NOT_FOUND,
            Self::Err(_) => Self::ERROR,
        }
    }
}

/// Object cache
pub struct ObjectCache {
    config: ConfigObjectCache,
    headers: HeaderMap,
    mc: Option<Arc<MetaCache>>,
}

impl ObjectCache {
    /// Create from config
    pub fn new(mut config: ConfigObjectCache, config_hk: Option<ConfigHousekeeper>) -> Result<Self, Error> {
        // create metacache only in internal cache mode
        let mc = {
            if config.mode == CacheMode::Internal {
                // path for cache storage
                let path = config.root
                    .take()
                    .ok_or(Error::Config("Cache root config param not defined".to_string()))?;           
                Some(Arc::new(
                    MetaCache::new(
                        path, 
                        config.in_memory_items.unwrap_or(10_000),
                        config_hk
                    )
                    .map_err(|e| Error::Internal(format!("Cache create error: {}", e)))?
                ))
            } else {
                None
            }
        }; 
        // cache-control headers
        let headers = config.make_headers();      
        Ok(Self {
            config,
            headers,
            mc
        })
    }

    /// Get cache-control headers for response
    pub fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }

    /// Get object from cache or from origin source, bypass if mode != Internal
    pub async fn get_object<'a, K, F, Fut>(&self, key: K, origin: F, condition: ConditionalHeaders)
    -> Result<DataObject, Error> 
    where 
        K: AsRef<ObjectKey> + std::marker::Copy,
        F: FnOnce(ConditionalHeaders)->Fut,
        Fut: Future<Output = Result<DataObject, Error>>
    {
        let mc = match self.mc {
            Some(ref mc) => mc,
            None => {
                // no cache, short circuit to origin 
                return origin(condition).await
            }
        };
                
        // make cache item key
        let key = key.as_ref().cache_key();
        
        // try to get object from cache
        let (obj, sri) = try_get_object(Arc::clone(mc), &key)
            .await? 
            .map_or((None, None), |(obj, sri)| (Some(obj), Some(sri)));
        
        // validate cache object
        let res = self.validate(obj, origin).await;
        let status = res.as_str();
        
        // modify cache (if nessesary) and return result
        match res {
            ValidationResult::Origin(obj) => {
                // got new object from origin
                // save stream to cache
                let stream = save_stream(
                    Arc::clone(mc), 
                    key, 
                    obj.stream, 
                    &obj.meta
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
                // make metadata for update
                let mut md = Metadata::new(key);
                md.metadata = json!(obj.meta);
                mc.update(md).await;
                // return from cache
                check_condition(obj, condition)
            },
            ValidationResult::Updated(obj) => {
                // got updated object from origin
                // remove old object from cache
                let sri = sri
                    .ok_or(Error::Internal("Error get sri while updating object in cache".to_string()))?;
                mc.remove_hash(&sri)
                    .await
                    .map_err(|e| Error::Internal(format!("Error remove object: {e}")))?;
                // save new object to cache
                let stream = save_stream(
                    Arc::clone(mc), 
                    key, 
                    obj.stream, 
                    &obj.meta
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
                    .ok_or(Error::Internal("Error get sri while removing object from cache".to_string()))?;
                mc.remove_hash(&sri)
                    .await
                    .map_err(|e| Error::Internal(format!("Error remove object: {e}")))?;
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
        F: FnOnce(ConditionalHeaders)->Fut,
        Fut: Future<Output = Result<DataObject, Error>>
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
                    Err(Error::NotModified(_)) => ValidationResult::Revalidated(obj),
                    Err(Error::NotFound(msg)) => ValidationResult::NotFound(
                        Error::NotFound(format!("Object was removed from origin, error message: {msg}"))
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

    pub async fn shutdown(&self) {
        if let Some(ref mc) = self.mc {
            mc.shutdown().await;        
        }
    }
}

fn check_condition(obj: DataObject, condition: ConditionalHeaders) 
-> Result<DataObject, Error> {
    // check etag
    if let Some(req_etag) = condition.if_none_match {
        if let Some(ref obj_etag) = obj.meta.etag {
            if &req_etag == obj_etag {
                // not modified
                return Err(Error::NotModified(String::new()))
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
                return Err(Error::NotModified(String::new()))
            }
        }
    }
    // modified
    Ok(obj)
}

async fn try_get_object(mc: Arc<MetaCache>, key: &str)
 -> Result<Option<(DataObject, Integrity)>, Error> {
    if let Some(md) = mc.metadata_checked(key).await {
        let sri = md.integrity;
        // decode meta from json
        let meta: ObjectMeta = serde_json::from_value(md.metadata)?;
        // get stream from cache and construct object
        let stream = read_stream(mc, sri.to_owned());
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
}

fn save_stream<K>(
    mc: Arc<MetaCache>,
    key: K,
    input: DataStream,
    meta: &ObjectMeta
) -> DataStream 
where 
    String: From<K> 
{
    // create metadata info for stream
    let mut md = Metadata::new(key.into());
    md.size = meta.content_length; // for check actual written size in commit
    md.metadata = json!{meta};

    let stream = try_stream! {
        // create writer for store stream in cache
        let mut fd = mc.writer(&md)
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;
        // read values from input
        for await value in input {
            if let Ok(ref value) = value {
                // write value to cache file
                fd.write_all(value)
                    .await
                    .map_err(S3Error::Io)?;
            }
            // yield to stream
            yield value?;
        }
        // check size and commit in cache
        fd.commit()
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;
    };
    // return pinned stream
    Box::pin(stream)   
}

fn read_stream(
    mc: Arc<MetaCache>, 
    sri: Integrity
) -> DataStream {
    let stream = try_stream! {
        // open cache for reading
        let mut fd = mc.reader(sri)
            .await
            .map_err(|e| S3Error::Http(500, e.to_string()))?;
        // read values from cache
        let mut buf = BytesMut::with_capacity(4096 * 16);    
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
    use async_stream::stream;
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
        let dir = "./test-data/cache-1";
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
        let dir = "./test-data/cache-2";
        let key = ObjectKey {
            bucket: "bucket1".to_owned(),
            key: "my key".to_owned()
        }.cache_key();
        let mc: Arc<MetaCache> = Arc::new(
            MetaCache::new(PathBuf::from(dir), 10, None)
            .unwrap());

        let meta1 = ObjectMeta { 
            content_type: Some("text/html".to_string()), 
            content_length: Some(80),
            date: Some(SystemTime::now()),
            last_modified: Some(SystemTime::now()),
            etag: Some("some-etag".to_string()), 
        };

        // input stream
        let input = Box::pin(
            stream! {
                for i in 0..10 {
                    let item: StreamItem = Ok(Bytes::from(format!("hello {}\n", i)));
                    yield item
                }
            }
        );

        // save input to cache and return persisted stream
        let saved = save_stream(Arc::clone(&mc), &key, input, &meta1);
        // read to buffer1
        let data1 = {
            let mut reader = StreamReader::new(saved);
            let mut data = vec![];
            reader.read_to_end(&mut data).await.unwrap();
            data
        };
      
        // get stream from cache
        let md = cacache::metadata(dir, &key).await.unwrap().unwrap();
        let sri = md.integrity;
        let meta2: ObjectMeta = serde_json::from_value(md.metadata).unwrap();
        let cached = read_stream(Arc::clone(&mc), sri);
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
        mc.shutdown().await;
        cacache::clear(dir).await.unwrap();
    }

    #[tokio::test]
    async fn object_meta_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "12096836".parse().unwrap());
        headers.insert("content-type", "image/jpeg".parse().unwrap());
        headers.insert("date", HttpDate::from(SystemTime::now()).to_string().parse().unwrap());
        headers.insert("last-modified", "Fri, 10 Feb 2023 09:57:33 GMT".parse().unwrap());
        headers.insert("etag", "76febaf5c48e1e2c834c6663d0cbedcb".parse().unwrap());

        let meta = ObjectMeta::from(&headers);

        assert_eq!(headers, meta.headers());
    }
}