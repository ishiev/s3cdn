use rocket::{
    http::Header,
    request::Request,
    response::{Responder, Response, Result},
};
use serde::{
    Deserialize, 
    Serialize,
};
use std::{
    path::PathBuf, 
    marker::PhantomData,
};
use cacache;


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
    pub use_stale: Option<bool>,
    pub background_update: Option<u64>,
}

impl ConfigObjectCache {
    fn make_headers(&self) -> Headers {
        if self.mode == CacheMode::External {
            // default 10 sec fresh
            let max_age = self.max_age.unwrap_or(10); 
            // default not use stale resource
            let use_stale = self.use_stale.unwrap_or(false);
            let stale_directive = if use_stale {
                // use stale resource for 1 day
                ", stale-while-revalidate=86400, stale-if-error=86400"
            } else {
                // not use stale resource, must revalidate or return error
                ", must-revalidate"
            };
            vec![
                Header::new("cache-control", format!("max-age={max_age}{stale_directive}"))
            ]
        } else {
            vec![]
        }
    }
}

#[derive(Debug)]
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
    use super::*;

    #[tokio::test]
    async fn double_write_cache() {
        let dir = String::from("./my-cache");

        // Write some data!
        cacache::write(&dir, "key", b"my-async-data").await.unwrap();
        // Write some data!
        cacache::write(&dir, "key", b"my-async-data2").await.unwrap();

        // Get the data back!
        let data = cacache::read(&dir, "key").await.unwrap();
        assert_eq!(data, b"my-async-data2");

        // Clean up the data!
        cacache::clear(&dir).await.unwrap();
    }
}