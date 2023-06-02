use std::{
    path::{PathBuf, Path}, 
    collections::{BinaryHeap, BTreeMap}, 
    ops::{
        AddAssign,
        Bound::{Included, Unbounded},
    },
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

/// Content storage entry, ordered by time
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
                .map(|x| x.modified().ok())
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

// Oldest entry first collection
type OldestFirstHeap = BinaryHeap<Reverse<TimeOrdEntry>>;

// Stat
#[derive(Default, Debug)]
struct DirStat {
    count: u64,
    size: u64,
}

impl AddAssign for DirStat {
    fn add_assign(&mut self, rhs: Self) {
        *self = Self {
            count: self.count + rhs.count,
            size: self.size + rhs.size,
        };
    }
}

impl From<(u64, u64)> for DirStat {
    fn from(value: (u64, u64)) -> Self {
        Self {
            count: value.0,
            size: value.1
        }
    }
}

/// Main housekeeper function
/// (run in blocking context)
fn housekeep(path: &Path, max_size: u64, max_duration: Option<Duration>) {
    // walk content dir tree and collect files
    let walkdir = WalkDir::new(path).follow_links(true);
    let mut files = BinaryHeap::new();
    
    debug!("Housekeeper: start checking cache storage...");
    let mut dir_stat = DirStat::default();
    for entry in walkdir {
        match entry {
            Ok(entry) if entry.file_type().is_file() => {
                let entry = TimeOrdEntry::from(entry);
                debug!("Houkeeper: found {:?}, size: {}, time: {:?}", entry.path, entry.size, entry.time);
                dir_stat += (1, entry.size).into();
                // sort min first
                files.push(Reverse(entry))
            },
            Ok(_) => continue,
            Err(e) => {
                error!("Housekeeper: error reading storage: {e}")
            }
        }
    }
    debug!("Housekeeper: found {} files, storage size {} bytes",
        dir_stat.count, dir_stat.size);

    // phase 1: remove to time
    let mut removed_stat = DirStat::default();
    if let Some(max_duration) = max_duration {
        let time_to_remove = SystemTime::now() - max_duration;
        removed_stat += remove_to_time(&mut files, time_to_remove);
    }

    // phase 2: check directory size and clean up 
    let size_to_remove = (dir_stat.size - removed_stat.size).saturating_sub(max_size);
    removed_stat += remove_to_size(&mut files, size_to_remove);

    if removed_stat.count == 0 {
        debug!("Housekeeper: complete, nothing has been done.");
        return;
    }
    debug!("Housekeeper: removed {} files, {} bytes",
        dir_stat.count, dir_stat.size);
    debug!("Housekeeper: complete, now we have {} files, storage size {} bytes", 
        dir_stat.count - removed_stat.count, 
        dir_stat.size - removed_stat.size
    );
}

/// Optimized file deletion procedure
/// removes the minimum number of oldest files 
fn remove_to_size(files: &mut OldestFirstHeap, mut size_to_remove: u64) -> DirStat {
    let mut size_tree = BTreeMap::new();
    let mut selected = 0u64;
    // select files from heap until given size and put it into b-tree map
    // tree key is sum of file sizes
    while selected < size_to_remove {
        let Some(entry) = files.pop().map(|x| x.0) 
        else { break };
        selected += entry.size;
        size_tree.insert(selected, entry);
    }
    // remove files until size_to_removed
    let mut removed_stat = DirStat::default();
    while size_to_remove > 0 {
        let key = { 
            // select file to remove with key equal or greater than size_to_remove
            let Some(entry) = size_tree
                .range((Included(size_to_remove), Unbounded))
                .next() 
            else { break };
            // remove file
            if let Err(e) = remove_file(&entry.1.path) {
                error!("Housekeeper: error removing cache file {:?}: {e}", &entry.1.path);
            } else {
                // calculate statictics
                removed_stat += (1, entry.1.size).into();
                // calculate rest to remove
                size_to_remove = size_to_remove.saturating_sub(entry.1.size);
            }
            // key value
            *entry.0
        };
        // remove selected file from tree
        // to prevent infinite loop in case of file removing errors
        // in this case other file may be selected in next interation
        size_tree.remove(&key);
    }
    // return stat
    removed_stat
}

/// Remove old files until given time
fn remove_to_time(files: &mut OldestFirstHeap, time_to_remove: SystemTime) -> DirStat {
    let mut removed_stat = DirStat::default();
    loop {
        let Some(entry) = files.pop() else { break };
        // check file time
        if entry.0.time < Some(time_to_remove) {
            // remove file if oldest than given
            if let Err(e) = remove_file(&entry.0.path) {
                error!("Housekeeper: error removing cache file {:?}: {e}", &entry.0.path);
            } else {
                // calculate statictics
                removed_stat += (1, entry.0.size).into();
            }
        } else {
            // push it back
            files.push(entry);
            break
        }
    }
    // return stat
    removed_stat
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

#[cfg(test)]
mod test {
    use super::*;
    use rand::{
        Rng, 
        distributions::Uniform
    };
    use std::{
        fs::{File, create_dir_all, }, 
        io::Write,
        thread::sleep
    };

    fn gen_rand_vec(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(33, 126);  // readable 7-bit ASCII symbols
        (0..size).map(|_| rng.sample(range)).collect()
    }

    fn gen_file(path: &Path, size: usize) {
        create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::create(path).unwrap();
        file.write_all(&gen_rand_vec(size)).unwrap()
    }

    #[inline]
    fn sleep_ms(millis: u64) {
        sleep(Duration::from_millis(millis))
    }

    #[test]
    fn housekeeping_size() {
        let base_path = PathBuf::from("test-data/housekeeper");

        // create some files, 110 bytes in summary
        gen_file(&base_path.join("a/01"), 5); sleep_ms(10);
        gen_file(&base_path.join("b/02"), 25); sleep_ms(10);
        gen_file(&base_path.join("c/03"), 30); sleep_ms(10);
        gen_file(&base_path.join("a/04"), 5); sleep_ms(10);
        gen_file(&base_path.join("c/05"),10); sleep_ms(10);
        gen_file(&base_path.join("c/06"),35); sleep_ms(10);

        // delele 35 bytes (110 - 75)
        housekeep(&base_path, 75, None);
        
        // check files (01 and 03 must be deleted)
        assert!(!base_path.join("a/01").exists());
        assert!(base_path.join("b/02").exists());
        assert!(!base_path.join("c/03").exists());
        assert!(base_path.join("a/04").exists());
        assert!(base_path.join("c/05").exists());
        assert!(base_path.join("c/06").exists());
    }
}