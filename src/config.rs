use rocket::http::uri::Origin;
use serde::{
    Deserialize, 
    Serialize,
};
use std::path::PathBuf;
use s3::{
    creds::Credentials,
    region::Region,
};
use url::Url;


/// Configuration params for server
#[derive(Debug, Deserialize, Serialize)]
pub struct Config<'a> {
    pub ident: String,
    pub base_path: Origin<'a>,
    pub storage: ConfigStorage,
    pub creds: Credentials,
    pub connection: ConfigConnection,
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
            creds: Credentials::anonymous().expect("Error create anonymous AWS credentials"),
            connection: ConfigConnection::default(),
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

/// Connection params
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct ConfigConnection {
    pub region: Option<Region>,
    pub endpoint: Option<Url>,
    pub pathstyle: bool,
    pub timeout: Option<u64>,
}

impl ConfigConnection {
    /// Make custom region variant from endpoint
    pub fn make_custom_region(&mut self) {
        self.region = Some(Region::Custom { 
            region: "".to_string(), 
            endpoint: if let Some(endpoint) = &self.endpoint {
                format!("{}:{}",
                    endpoint.host_str().unwrap_or_default(),
                    endpoint.port_or_known_default().unwrap_or(80)
                )
            } else {
                "".to_string()
            }
        })
    } 
}
