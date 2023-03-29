use std::{
    path::Path, 
    time::Duration, 
    sync::Arc,
};
use cacache::{
    WriteOpts, 
    Value, 
    Reader, 
    Writer,
    Error,
};
use moka::{
    future::Cache, 
    notification::RemovalCause, 
    Entry
};
use ssri::Integrity;


#[derive(Default, Debug, Clone)]
pub struct Metadata {
    // key this entry is stored under
    pub key: String,
    // integrity hash for the stored data. Acts as a key into {cache}/content
    pub integrity: Option<Integrity>,
    // timestamp in unix milliseconds when this entry was written
    pub time: Option<u128>,
    // size of data associated with this entry
    pub size: Option<usize>,
    // arbitrary JSON  associated with this entry
    pub metadata: Value,
}

/// Convert from cacache
impl From<cacache::Metadata> for Metadata {
    fn from(md: cacache::Metadata) -> Self {
        Self {
            key: md.key,
            integrity: Some(md.integrity),
            time: if md.time == 0 { None } else { Some(md.time) },
            size: if md.size == 0 { None } else { Some(md.size) },
            metadata: md.metadata,
        }
    }
}

/*
/// Convert back to cacache
impl From<Metadata> for cacache::Metadata {
    fn from(md: Metadata) -> Self {
        Self {
            key: md.key,
            integrity: md.integrity.unwrap_or_else(|| Integrity::from([0;0])),
            time: md.time,
            size: md.size,
            metadata: md.metadata,
            raw_metadata: None
        }
    }
}

*/

/// Convert to write options
impl From<Metadata> for WriteOpts {
    fn from(md: Metadata) -> Self {
        let mut opts = WriteOpts::new();
        if let Some(sri) = md.integrity {
            opts = opts.integrity(sri)
        }
        if let Some(size) = md.size {
            opts = opts.size(size)
        }
        if let Some(time) = md.time {
            opts = opts.time(time)
        }
        opts.metadata(md.metadata)
    }
}


#[derive(Debug, Clone)]
pub struct MetaCache<'a>  {
    path: &'a Path,
    cache: Cache<String, Metadata>
}

impl <'a> MetaCache<'a> {
    pub fn new(path: &'a Path, ttl: u64, capacity: u64) -> Self {
        // create eviction closure
        // eviction will save metadata to index file
        let cache = path.to_owned();
        let listener = 
            move | key: Arc<String>, md: Metadata, cause: RemovalCause | {
            // insert cached metadata to file
            let opts = md.into();
            if let Err(e) = cacache::index::insert(&cache, &key, opts) {
                error!("Error commit cache metadata: {}", e);
            } else {
                info!("Commit metadata for key: {}, reason: {:?}", &key, cause)
            }    
        };
        // create cache
        let cache = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(Duration::from_secs(ttl))
            .eviction_listener_with_queued_delivery_mode(listener)
            .build();
        // return cache
        MetaCache { 
            path, 
            cache 
        }
    }

    pub async fn save(&self, key: &str) {
        self.cache.invalidate(key).await
    } 

    pub fn save_all(&self) {
        info!("Start saving to index: {} elements...", self.cache.entry_count());
        self.cache.invalidate_all()
    }     

    pub async fn metadata(&self, key: &str) -> Option<Metadata> {
        // make async block for index reading
        // return None if reading error
        let init = async {
            cacache::index::find_async(self.path, key)
                .await
                .unwrap_or_else(|e| {
                    error!("Error read index: {}", e);
                    None
                })
                .map(Metadata::from)
        };
        // find entry in cache or read from index
        self.cache
            .entry_by_ref(key)
            .or_optionally_insert_with(init)
            .await
            .map(Entry::into_value) 
    }

    pub async fn metadata_checked(&self, key: &str) -> Option<cacache::Metadata> {
        if let Some(md) = self.metadata(key).await {
            if let Some(sri) = md.integrity {
                if cacache::exists(self.path, &sri).await {
                    let md = cacache::Metadata {
                        key: md.key,
                        integrity: sri,
                        time: md.time.unwrap_or_default(),
                        size: md.size.unwrap_or_default(),
                        metadata: md.metadata,
                        raw_metadata: None,
                    };
                    Some(md)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn exists(&self, sri: &Integrity) -> bool {
        cacache::exists(self.path, sri).await
    }

    pub async fn remove(&self, sri: &Integrity) -> Result<(), Error>{
        cacache::remove_hash(self.path, sri).await
    }

    pub async fn insert(&self, md: Metadata) {
        self.cache.insert(md.key.clone(), md).await
    }

    pub async fn writer(&self, md: Metadata) -> Result<Writer, Error> {
        WriteOpts::from(md).open_hash(self.path).await
    }

    pub async fn commit(&self, mut md: Metadata, writer: Writer) -> Result<Integrity, Error> {
        let sri = writer.commit().await?;
        md.integrity = Some(sri.clone());
        self.insert(md).await;
        Ok(sri)
    }

    pub async fn reader(&self, sri: Integrity) -> Result<Reader, Error> {
        Reader::open_hash(&self.path, sri).await
    }
}


impl Drop for MetaCache<'_> {
    fn drop(&mut self) {
        self.save_all()
    }
}