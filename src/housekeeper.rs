use std::{
    path::{PathBuf, Path}, 
    collections::BinaryHeap, 
    time::{SystemTime, Duration},
    cmp::Reverse, 
    fs::remove_file, 
    sync::Arc
};

use serde::{Deserialize, Serialize};
use tokio::{task, time, sync::RwLock, select};
use tokio_util::sync::CancellationToken;
use walkdir::{DirEntry, WalkDir};

/// Cacache content version
const CONTENT_VERSION: &str = "2";

/// Housekeerer config
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct ConfigHousekeeper {
    pub max_size: u64,                  // in bytes
    pub max_duration: Option<u64>,      // in seconds
    pub keeper_interval: Option<u64>,   // start interval in seconds, default 3600
}

/// Content storage entry 
/// Field ordering is important!
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct TimeOrdEntry {
    time: Option<SystemTime>, // access or modify time
    size: u64,                // entry size in bytes
    path: PathBuf,            // path to entry
}

impl From<DirEntry> for TimeOrdEntry {
    fn from(value: DirEntry) -> Self {
        let md = value.metadata();
        Self {
            // get access time (if any) or modified time or None
            time: md.as_ref()
                .map(|x| x.accessed().or(x.modified()).ok())
                .ok()
                .flatten(),            
            // get file len or 0
            size: md.as_ref()
                .map(|x| x.len())
                .unwrap_or(0),
            path: value.into_path(),
        }
    }
}

fn housekeep(path: &Path, max_size: u64, _max_duration: Option<Duration>) {
    // walk content dir tree and collect files
    let walkdir = WalkDir::new(path).follow_links(true);
    let mut files = BinaryHeap::new();
    
    debug!("Housekeeper: start checking cache storage...");
    let mut dir_size = 0u64;
    for entry in walkdir {
        match entry {
            Ok(entry) if entry.file_type().is_file() => {
                let entry = TimeOrdEntry::from(entry);
                debug!("Houkeeper: found {:?}, size: {}, time: {:?}", entry.path, entry.size, entry.time);
                dir_size += entry.size;
                // sort min first
                files.push(Reverse(entry))
            },
            Ok(_) => continue,
            Err(e) => {
                error!("Housekeeper: error reading storage: {e}")
            }
        }
    }
    debug!("Housekeeper: found {} files, storage size {} bytes", files.len(), dir_size);

    // check directory size and clean up 
    // until size is less than or equal to max_size
    let mut removed_count = 0u64;
    let mut removed_size = 0u64; 
    while dir_size > max_size {
        let Some(entry) = files.pop().map(|x| x.0) else { 
            break
        };
        if let Err(e) = remove_file(&entry.path) {
            error!("Housekeeper: error removing cache file {:?}: {e}", &entry.path);
        } else {
            removed_count += 1;
            removed_size += entry.size;
            dir_size -= entry.size;
        }
    }
    if removed_count == 0 {
        debug!("Housekeeper: complete, nothing has been done.");
        return;
    }
    debug!("Housekeeper: removed {removed_count} files, {removed_size} bytes");
    debug!("Housekeeper: complete, now we have {} files, storage size {} bytes", files.len(), dir_size);
}

#[derive(Debug)]
pub struct Housekeeper {
    task: RwLock<Option<(task::JoinHandle<()>, CancellationToken)>>,
}

impl Housekeeper {
    pub fn new(path: &Path, config: ConfigHousekeeper) -> Self {
        // make path to content storage
        let path = Arc::new(
            path.join(format!("content-v{CONTENT_VERSION}"))
        );
        // make cancel token
        let cancel_token = CancellationToken::new();
        let child_token = cancel_token.child_token();
        // create scheduler to start every interval
        let sheduler = async move {
            // make shedule interval
            let mut interval = time::interval(config
                .keeper_interval
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(3600))
            );
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            // get maximum storage duration 
            let max_duration = config
                .max_duration
                .map(Duration::from_secs);
            // start housekeeping every interval
            loop {
                select! {
                    _ = child_token.cancelled() => {
                        // cancel requested, exiting
                        break
                    }
                    _ = interval.tick() => {
                        // 
                        let path = Arc::clone(&path);
                        // start housekeeping as blocking task
                        let res = task::spawn_blocking(move || {
                            housekeep(&path, config.max_size, max_duration)
                        }).await;
                        if let Err(e) = res {
                            error!("Housekeeper task execution error {e}");
                        }
                    }
                }
            }
            debug!("Housekeeper task finished.");
        };
        // start task and save to struct option
        debug!("Housekeeper task started.");
        Self {
            task: RwLock::new(Some(
                (
                    // start sheduler as async task
                    task::spawn(sheduler),
                    cancel_token
                )
            ))
        }     
    }

    pub async fn exit(&self) {
        if let Some(task) = self.task.write().await.take() {
            // cancel request
            task.1.cancel();
            debug!("Waiting for housekeeper task...");
            // waiting for task exit
            task.0.await.ok();
        } else {
            warn!("Housekeeper task already exited.");
        }  
    }
}