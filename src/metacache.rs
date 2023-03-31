use std::{
    path::PathBuf, 
    time::{Duration, SystemTime, UNIX_EPOCH}, 
    sync::Arc,
};
use cacache::{
    WriteOpts, 
    Value, 
    Reader, 
    Writer,
    Error,
};
use lockfile::Lockfile;
use moka::{
    future::Cache, 
    future::ConcurrentCacheExt,
    notification::RemovalCause, 
    Entry
};
use ssri::Integrity;


#[derive(Default, Debug, Clone, PartialEq)]
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

/// Convert to write options
impl From<&Metadata> for WriteOpts {
    fn from(md: &Metadata) -> Self {
        let mut opts = WriteOpts::new();
        if let Some(sri) = md.integrity.clone() {
            opts = opts.integrity(sri)
        }
        if let Some(size) = md.size {
            opts = opts.size(size)
        }
        if let Some(time) = md.time {
            opts = opts.time(time)
        }
        opts.metadata(md.metadata.clone())
    }
}


pub struct MetaCache  {
    path: PathBuf,
    cache: Cache<String, Metadata>,
    _lock: Lockfile
}

impl MetaCache {
    pub fn new(path: PathBuf, ttl: u64, capacity: u64) -> Result<Self, Error> {
        // try to lock cache path
        let lock = {
            let mut lockfile = PathBuf::from(&path);
            lockfile.push(".lockfile");
            // create lockfile with all parents dir
            Lockfile::create_with_parents(lockfile)
                .map_err(|e| 
                    Error::IoError(
                        e.into_inner(), 
                        format!("Cannot lock cache directory: {:?}. \
                                Maybe another process is already running?", &path)
                    )
                )?
        };
        // create eviction closure
        // eviction will save metadata to index file
        let cache = path.clone();
        let listener = 
            move | key: Arc<String>, md: Metadata, cause: RemovalCause | {
                // replace only in-memory
                if cause != RemovalCause::Replaced {
                    // maybe it already exists?
                    if let Some(index_md) = cacache::index::find(&cache, &key)
                            .ok()
                            .flatten() {
                        if Metadata::from(index_md) == md {
                            // already saved, do nothing
                            info!("Drop metadata for key: {}, reason: {:?}", &key, cause);
                            return;
                        }
                    }
                    // insert cached metadata to index
                    let opts = WriteOpts::from(&md);
                    if let Err(e) = cacache::index::insert(&cache, &key, opts) {
                        error!("Error commit cache metadata: {}", e);
                    } else {
                        info!("Save metadata for key: {}, reason: {:?}", &key, cause)
                    }
                }
            };
        // create cache
        let cache = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(Duration::from_secs(ttl))
            .eviction_listener_with_queued_delivery_mode(listener)
            .build();
        // return cache
        Ok(MetaCache { 
            path, 
            cache,
            _lock: lock
        })
    }

    pub fn save_all(&self) {
        info!("Start saving metadata to index: {} element(s)...", self.cache.entry_count());
        self.cache.invalidate_all();
        self.cache.sync();
    }     

    pub async fn metadata(&self, key: &str) -> Option<Metadata> {
        // make async block for index reading
        // return None if reading error
        let init = async {
            cacache::index::find_async(&self.path, key)
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
                if self.exists(&sri).await {
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
        cacache::exists(&self.path, sri).await
    }

    pub async fn remove_hash(&self, sri: &Integrity) -> Result<(), Error>{
        cacache::remove_hash(&self.path, sri).await
    }

    pub async fn insert(&self, md: Metadata) {
        self.cache.insert(md.key.clone(), md).await
    }

    pub async fn update(&self, md: Metadata) {
        if let Some(mut item) = self.cache.get(&md.key) {
            item.integrity = md.integrity.or(item.integrity);
            item.time = md.time.or(item.time);
            item.size = md.size.or(item.size);
            if !md.metadata.is_null() {
                item.metadata = md.metadata
            }
            self.insert(item).await
        }
    }

    pub async fn writer(&self, md: &Metadata) -> Result<Writer, Error> {
        WriteOpts::from(md).open(&self.path, &md.key).await
    }

    pub async fn commit(&self, mut md: Metadata, writer: Writer) -> Result<Integrity, Error> {
        let sri = writer.commit().await?;
        md.integrity = Some(sri.clone());
        md.time = if md.time.is_none() { Some(now()) } else { md.time} ;
        self.insert(md).await;
        Ok(sri)
    }

    pub async fn reader(&self, sri: Integrity) -> Result<Reader, Error> {
        Reader::open_hash(&self.path, sri).await
    }
}

impl Drop for MetaCache {
    fn drop(&mut self) {
        self.save_all()
    }
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}