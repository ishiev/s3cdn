use rocket::http::uri::Origin;
use rocket::serde::{Deserialize, Serialize};
use std::path::PathBuf;


/// Configuration params for server
#[derive(Debug, Deserialize, Serialize)]
pub struct Config<'a> {
    pub ident: String,
    pub base_path: Origin<'a>,
    pub storage: ConfigStorage,
}

impl Default for Config<'_> {
    fn default() -> Self {
        Config {
            ident: format!("{}/{}", 
                env!("CARGO_PKG_NAME"), 
                env!("CARGO_PKG_VERSION")
            ),
            base_path: Origin::path_only("/"),
            storage: ConfigStorage::default(),
        }
    }
}

/// Storage params
#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigStorage {
    pub root: PathBuf,
}

impl Default for ConfigStorage {
    fn default() -> Self {
        ConfigStorage {
            root: PathBuf::from("data"),
        }
    }
}
