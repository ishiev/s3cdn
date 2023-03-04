use serde::{
    Deserialize, 
    Serialize,
};
use std::{
    path::PathBuf, 
};
use cacache;


/// Cache mode
#[derive(Default, Debug, Deserialize, Serialize)]
pub enum CacheMode {
    Off,
    #[default]
    External,
    Internal
}

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

#[derive(Debug)]
pub struct ObjectCache {
    config: ConfigObjectCache,
}

impl From<ConfigObjectCache> for ObjectCache {
    fn from(config: ConfigObjectCache) -> Self {
        Self {
            config
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