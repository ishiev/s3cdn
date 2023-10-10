use std::net::SocketAddr;
use serde::{
    Deserialize, 
    Serialize,
};
use s3::{
    creds::Credentials,
    region::Region,
};
use http::Uri;
use crate::{
    cache::ConfigObjectCache, 
    housekeeper::ConfigHousekeeper
};


/// Configuration params for server
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub address: SocketAddr,
    #[serde(with = "http_serde::uri")]
    pub base_path: Uri,
    pub cache: Option<ConfigObjectCache>,
    pub housekeeper: Option<ConfigHousekeeper>,
    pub creds: Credentials,
    pub connection: ConfigConnection,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127,0,0,1], 8000)),
            base_path: Default::default(),
            cache: Default::default(),
            housekeeper: Default::default(),
            creds: Credentials::anonymous().expect("error create anonymous AWS credentials"),
            connection: Default::default(),        
        }
    }
}

/// Connection params
#[derive(Default, Debug, Deserialize, Serialize)]
pub struct ConfigConnection {
    pub region: Option<Region>,
    #[serde(with = "http_serde::uri")]
    pub endpoint: Uri,
    pub pathstyle: bool,
    pub timeout: Option<u64>,
}

impl ConfigConnection {
    /// Make custom region variant from endpoint
    pub fn make_custom_region(&mut self) {
        self.region = Some(Region::Custom { 
            region: "".to_string(), 
            endpoint: 
                format!("{}://{}:{}",
                    self.endpoint.scheme_str().unwrap_or("https"),
                    self.endpoint.host().unwrap_or_default(),
                    self.endpoint.port_u16().unwrap_or(80)
                )
        })
    } 
}
