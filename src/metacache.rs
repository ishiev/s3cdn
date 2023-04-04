use std::{
    path::{PathBuf, Path}, 
    time::{Duration, SystemTime, UNIX_EPOCH}, 
    sync::Arc, borrow::Cow,
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
    notification::RemovalCause, 
    Entry
};
use ssri::Integrity;
use tokio::{
    sync::{mpsc::{self, error::SendError}, RwLock}, 
    task::{self, JoinHandle}
};

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

impl Metadata {
    /// Save metadata to index file
    async fn save(&self, cache: &Path, reason: &str) {
        // maybe it already exists?
        if let Some(index_md) = cacache::index::find_async(cache, &self.key).await
                .ok()
                .flatten() {
            if Metadata::from(index_md) == *self {
                // already saved, do nothing
                debug!("Drop metadata for key: {}, reason: {}", &self.key, reason);
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
    Exit                        // exit writer 
}

struct IndexWriter {
    task: RwLock<Option<JoinHandle<()>>>,
    tx: mpsc::Sender<WriterCommand>
}

impl IndexWriter {
    fn new(path: PathBuf) -> Self {
        // create index writer channel
        let (tx, mut rx) = mpsc::channel(5000);      
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
            debug!("Writer task already exited.");
        }   
    }

    async fn write(&self, md: Arc<Metadata>, reason: Cow<'static, str>) -> Result<(), SendError<WriterCommand>> {
        self.tx.send(WriterCommand::Write(md, reason)).await
    }

    fn sender(&self) -> mpsc::Sender<WriterCommand> {
        self.tx.clone()
    }
}

pub struct MetaCache  {
    path: PathBuf,
    cache: Cache<String, Arc<Metadata>>,
    writer: IndexWriter,
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

        // create index writer
        let writer = IndexWriter::new(path.clone());

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
            .time_to_live(Duration::from_secs(ttl))
            .eviction_listener_with_queued_delivery_mode(listener)
            .build();
        
        // return cache
        Ok(MetaCache { 
            path, 
            cache,
            writer,
            _lock: lock
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
        self.writer.exit().await;
        info!("Cache shutdown complete.");
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
                .map(Arc::new)
        };
        // find entry in cache or read from index
        self.cache
            .entry_by_ref(key)
            .or_optionally_insert_with(init)
            .await
            .map(Entry::into_value)
            .map(|x| x.as_ref().clone())
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
                    return Some(md)
                } 
            }
        }
        None
    }

    pub async fn exists(&self, sri: &Integrity) -> bool {
        cacache::exists(&self.path, sri).await
    }

    pub async fn remove_hash(&self, sri: &Integrity) -> Result<(), Error>{
        cacache::remove_hash(&self.path, sri).await
    }

    pub async fn insert(&self, md: Metadata) {
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


fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}