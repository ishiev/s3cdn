use std::{
    path::{PathBuf, Path}, 
    time::SystemTime, 
    sync::Arc, 
    borrow::Cow,
    time::Duration,
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
};
use ssri::Integrity;
use tokio::{
    sync::{mpsc::{self, error::SendError}, RwLock}, 
    task::{self, JoinHandle}
};
use sha1::{Sha1, Digest};

use crate::housekeeper::Housekeeper;

/// Cacache index version
const INDEX_VERSION: &str = "5";

/// Index file metadata
#[derive(Debug, Clone, PartialEq)]
struct IndexMeta {
    // file size in bytes
    len: u64,
    // file last modified time
    modified: Option<SystemTime>
}

impl From<std::fs::Metadata> for IndexMeta {
    fn from(value: std::fs::Metadata) -> Self {
        Self { 
            len: value.len(),
            modified: value.modified().ok()
        }
    }
} 

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
    // index file metadata (private field for internal use)
    index_meta: Option<IndexMeta>,
}

impl Metadata {
    /// Create from string key
    pub fn new(key: String) -> Self {
        Metadata { 
            key,
            ..Default::default()
        }
    }

    /// Read metadata from index file
    async fn read(cache: &Path, key: &str) -> Result<Option<Self>, Error> {
        // get index file metadata
        let index_meta = index_meta(cache, key)
            .await
            .map_err(|e| Error::IoError(e, "Error reading metadata".to_string()))?;
        // read index file 
        let md = cacache::index::find_async(cache, key)
            .await?
            .map(|x| {
                let mut md = Metadata::from(x);
                md.index_meta = Some(index_meta);
                md
            });
        Ok(md)
    }

    /// Save metadata to index file
    async fn save(&self, cache: &Path, reason: &str) {
        // maybe already saved?
        if let Some(index_md) = cacache::index::find_async(cache, &self.key).await
                .ok()
                .flatten() {
            let mut md = Metadata::from(index_md);
            md.index_meta = index_meta(cache, &self.key).await.ok();
            if md == *self {
                // already saved, do nothing
                debug!("Drop metadata for key: {} (already saved), reason: {}", &self.key, reason);
                return
            } else if Some(md.time) > Some(self.time) {
                // saved medatada is newer than our, do nothing
                debug!("Drop metadata for key: {} (older than saved), reason: {}", &self.key, reason);
                return
            }
        }
        // insert to index
        let opts = WriteOpts::from(self);
        if let Err(e) = cacache::index::insert_async(cache, &self.key, opts).await {
            error!("Error save cache metadata: {}", e);
        } else {
            debug!("Save metadata for key: {}, reason: {}", &self.key, reason)
        }
    }
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
            index_meta: None
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

enum WriterCommand {
    Write(Arc<Metadata>, Cow<'static, str>),    // write metadata to index with reason
    Exit                                        // exit writer 
}

#[derive(Debug)]
struct IndexWriter {
    task: RwLock<Option<JoinHandle<()>>>,
    tx: mpsc::Sender<WriterCommand>
}

impl IndexWriter {
    fn new(path: &Path) -> Self {
        // create index writer channel
        let (tx, mut rx) = mpsc::channel(5000);      
        // make owned path for async task
        let path = path.to_owned();
        // spawn a async task
        // task ended when the channel has been closed or Exit command received
        let task = task::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    WriterCommand::Write(md, reason) => md.save(&path, &reason).await,
                    WriterCommand::Exit => break
                }
            }
            debug!("Index writer task finished.");
        });
        Self { task: RwLock::new(Some(task)), tx }
    }

    async fn exit(&self) {
        if let Err(e) = self.tx.send(WriterCommand::Exit).await {
            // receiver dropped
            error!("Writer task receiver dropped: {}", e);         
            return
        }
        if let Some(task) = self.task.write().await.take() {
            debug!("Waiting for index writer task...");
            task.await.ok();
        } else {
            warn!("Writer task already exited.");
        }   
    }

    async fn write(&self, md: Arc<Metadata>, reason: Cow<'static, str>) -> Result<(), SendError<WriterCommand>> {
        self.tx.send(WriterCommand::Write(md, reason)).await
    }

    fn sender(&self) -> mpsc::Sender<WriterCommand> {
        self.tx.clone()
    }
}

#[derive(Debug)]
pub struct MetaCache  {
    path: PathBuf,
    cache: Cache<String, Arc<Metadata>>,
    writer: IndexWriter,
    keeper: Option<Housekeeper>,
}

impl MetaCache {
    pub fn new(path: PathBuf, capacity: u64, max_size: Option<u64>, interval: Option<Duration>) -> Result<Self, Error> {
        // create index writer
        let writer = IndexWriter::new(&path);

        // create housekeeper if max_size provided
        let keeper = max_size.map(
            |max_size| Housekeeper::new(
                &path, 
                max_size,
                interval.unwrap_or_else(|| Duration::from_secs(3600)),                
            )
        );

        // create eviction closure
        // eviction will save metadata to index file
        let tx = writer.sender();
        let listener = 
            move | _key: Arc<String>, md: Arc<Metadata>, cause: RemovalCause | {
                // replace only in-memory
                if cause != RemovalCause::Replaced {
                    tx.blocking_send(
                        WriterCommand::Write(md, Cow::from(format!("{:?}", cause))))
                        .unwrap_or_else(|e| {
                            error!("Error save cache metadata: {}", e);
                        })
                }
            };
        
        // create cache
        let cache = Cache::builder()
            .max_capacity(capacity)
            .eviction_listener_with_queued_delivery_mode(listener)
            .build();
        
        // return cache
        Ok(MetaCache { 
            path, 
            cache,
            writer,
            keeper,
        })
    }

    pub async fn shutdown(&self) {
        info!("Start cache shutdown, saving metadata to index: {} element(s)...", self.cache.entry_count());
        for (_key, md) in self.cache.iter() {
            self.writer.write(md, Cow::from("Shutdown"))
                .await
                .unwrap_or_else(|e| {
                    error!("Error send save command: {}", e);
                });
        }
        // exit writer task
        self.writer.exit().await;
        // exit housekeeper task
        if let Some(ref keeper) = self.keeper {
            keeper.exit().await
        }
        info!("Cache shutdown complete.");
    }

    pub async fn metadata(&self, key: &str) -> Option<Metadata> {
        // make async block for index reading
        // return None if reading error
        let init = || async {
            Metadata::read(&self.path, key)
                .await
                .unwrap_or_else(|e| {
                    error!("Error read index: {}", e);
                    None
                })
                .map(Arc::new)
        };
        // find entry in cache or read from index
        let entry = self.cache
            .entry_by_ref(key)
            .or_optionally_insert_with(init())
            .await?;     
        
        let md = entry.value().as_ref().clone();
        //  return value if entry is fresh (just readed from index)
        if entry.is_fresh() {
            return Some(md)
        }
        // check index file if entry not fresh
        let index_meta = index_meta(&self.path, key).await.ok();
        if index_meta == md.index_meta {
            Some(md)
        } else {
            // read metadata from index file
            let index_md = init().await?;
            self.cache.insert(index_md.key.clone(), Arc::clone(&index_md)).await;
            Some(index_md.as_ref().clone())
        }       
    }

    pub async fn metadata_checked(&self, key: &str) -> Option<cacache::Metadata> {
        // get metadata from cache 
        let md = self.metadata(key).await?;    
        // check for sri
        let sri = md.integrity?;
        // check for data file
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
    }

    pub async fn exists(&self, sri: &Integrity) -> bool {
        cacache::exists(&self.path, sri).await
    }

    pub async fn remove_hash(&self, sri: &Integrity) -> Result<(), Error>{
        cacache::remove_hash(&self.path, sri).await
    }

    pub async fn insert_local(&self, md: Metadata) {
        self.cache.insert(md.key.clone(), Arc::new(md)).await
    }

    pub async fn update(&self, md: Metadata) {
        if let Some(mut item) = self.cache
                .get(&md.key)
                .map(|x| x.as_ref().clone()) {
            item.integrity = md.integrity.or(item.integrity);
            item.time = md.time.or(item.time);
            item.size = md.size.or(item.size);
            if !md.metadata.is_null() {
                item.metadata = md.metadata
            }
            self.insert_local(item).await
        }
    }

    pub async fn writer(&self, md: &Metadata) -> Result<Writer, Error> {
        WriteOpts::from(md).open(&self.path, &md.key).await
    }

    pub async fn reader(&self, sri: Integrity) -> Result<Reader, Error> {
        Reader::open_hash(&self.path, sri).await
    }
}

/// From cacache::index
fn bucket_path(cache: &Path, key: &str) -> PathBuf {
    let hashed = hash_key(key);
    cache
        .join(format!("index-v{INDEX_VERSION}"))
        .join(&hashed[0..2])
        .join(&hashed[2..4])
        .join(&hashed[4..])
}

/// From cacache::index
fn hash_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key);
    hex::encode(hasher.finalize())
}

/// Index file metadata
async fn index_meta(cache: &Path, key: &str) -> std::io::Result<IndexMeta> {
    tokio::fs::metadata(bucket_path(cache, key))
        .await
        .map(IndexMeta::from)
} 


#[cfg(test)]
mod test {
    use super::*;
    use rand::{
        Rng, 
        distributions::Uniform
    };
    use serde_json::json;
    use tokio::{time::Instant, io::{AsyncWriteExt, AsyncReadExt}};

    #[allow(dead_code)]
    fn gen_rand_vec(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, 255);

        (0..size).map(|_| rng.sample(range)).collect()
    }

    fn now() -> u128 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    fn gen_metadata(key: String) -> Metadata {
        Metadata { 
            integrity: Some(Integrity::from(&key)),
            time: Some(now()),
            size: Some(key.len()),
            metadata: json!(key),
            index_meta: None,
            key
        }
    }

    #[tokio::test]
    async fn double_instance() {
        let path = PathBuf::from("./test-data/metacache-1");
        let key = "MyData".to_string();
        let test_data = "My test data".as_bytes();

        // write some data to cache instrance 1
        let mc1 = MetaCache::new(path.clone(), 1000, None, None).unwrap();
        let md1 = Metadata::new(key.clone());
        let mut writer = mc1.writer(&md1).await.unwrap();
        writer.write_all(test_data).await.unwrap();
        writer.commit().await.unwrap();
        
        // read data from cache instance 2
        let mc2 = MetaCache::new(path, 1000, None, None).unwrap();
        let md2 = mc2.metadata_checked(&key).await.unwrap();
        let mut reader = mc2.reader(md2.integrity).await.unwrap();
        let mut result = Vec::<u8>::new();
        reader.read_to_end(&mut result).await.unwrap();

        assert_eq!(test_data, result)
    }

    #[tokio::test]
    /// Test index write/read performance
    async fn metacache_insert_perf() {
        const ITEMS: usize = 10_000;
        let path = PathBuf::from("./test-data/metacache-2");
        let mc = Arc::new(
            MetaCache::new(path.clone(), ITEMS as u64, None, None)
            .unwrap());

        let keys: Vec<String> = (0..ITEMS)
            .map(|x| format!("key {x}"))
            .collect();
        
        let md1: Vec<Metadata> = keys
            .iter()
            .map(|x| gen_metadata(x.clone()))
            .collect();

        // async tasks for inserts
        let tasks: Vec<JoinHandle<()>> = md1
            .iter()
            .map(|x| {
                let mc = Arc::clone(&mc);
                let md = x.clone();
                task::spawn(async move {
                    mc.insert_local(md).await
                })
            })
            .collect();

        // complete all
        for h in tasks {
            let _ = h.await;
        }

        // saving to index
        println!("start saving cache to index...");
        let t0 = Instant::now();
        mc.shutdown().await;
        let t_write = t0.elapsed().as_secs_f32();
        println!(">> complete in {:.2}s", t_write);
        assert!(t_write < 20.0);

        // now we can get it back
        println!("start reading cache from index...");
        let t0 = Instant::now();
        let mut md2: Vec<Metadata> = Vec::with_capacity(ITEMS);
        for x in keys.iter() {
            md2.push(async {
                let mut md = mc.metadata(x.as_str()).await.unwrap();
                // erase index file meta to correct compare vectors
                md.index_meta = None;
                md
            }.await);
        }
        let t_read = t0.elapsed().as_secs_f32();
        println!(">> complete in {:.2}s", t_read);
        assert!(t_read < 20.0);

        assert_eq!(md1, md2);
    }

    #[tokio::test]
    /// Get fresh metadata after index file update
    async fn check_index_update() {
        let path = PathBuf::from("./test-data/metacache-3");
        let mc = MetaCache::new(path.clone(), 10, None, None).unwrap();
        let key = "MyData".to_string();
        let test_data_1 = "My test data".as_bytes();
        let test_data_2 = "My another test data".as_bytes();

        // write some data to cache
        let md = Metadata::new(key.clone());
        let mut writer = mc.writer(&md).await.unwrap();
        writer.write_all(test_data_1).await.unwrap();
        writer.commit().await.unwrap();

        // test_1: get back data
        let md1 = mc.metadata_checked(&key).await.unwrap();
        let mut reader = mc.reader(md1.integrity.clone()).await.unwrap();
        let mut result = Vec::<u8>::new();
        reader.read_to_end(&mut result).await.unwrap();
        
        assert_eq!(test_data_1, result);

        // rewrite data in the same key
        let mut writer = mc.writer(&md).await.unwrap();
        writer.write_all(test_data_2).await.unwrap();
        writer.commit().await.unwrap();

        // test_2: get back new metadata and new data
        let md2 = mc.metadata_checked(&key).await.unwrap();
        assert_ne!(md1, md2);
        let mut reader = mc.reader(md2.integrity).await.unwrap();
        let mut result = Vec::<u8>::new();
        reader.read_to_end(&mut result).await.unwrap();

        assert_eq!(test_data_2, result);
    }
}