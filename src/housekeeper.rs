use std::{
    path::PathBuf, 
    collections::BinaryHeap, 
    time::SystemTime,
    cmp::Reverse, 
    fs::remove_file
};

use rocket::log::private::error;
use tokio::task::JoinHandle;
use walkdir::{DirEntry, WalkDir};

/// Cacache content version
const CONTENT_VERSION: &str = "2";

/// Content storage entry 
/// Field ordering is important!
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    time: Option<SystemTime>, // access or modify time
    size: u64,                // entry size in bytes
    path: PathBuf,            // path to entry
}

impl From<DirEntry> for Entry {
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

fn clean_dir(mut path: PathBuf, max_size: u64) {
    // walk content dir tree and collect files
    path.push(format!("content-v{CONTENT_VERSION}"));
    let walkdir = WalkDir::new(path).follow_links(true);
    let mut files = BinaryHeap::new();
    
    debug!("Housekeeper: start checking cache storage...");
    let mut dir_size = 0u64;
    for entry in walkdir {
        match entry {
            Ok(entry) if entry.file_type().is_file() => {
                let entry = Entry::from(entry);
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

pub fn start_housekeeping<P>(path: P, max_size: u64) -> JoinHandle<()> 
where PathBuf: From<P> {
    let path = path.into();
    tokio::task::spawn_blocking(move || {
        clean_dir(path, max_size)
    })
}