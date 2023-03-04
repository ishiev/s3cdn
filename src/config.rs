use rocket::http::uri::Origin;
use serde::{
    Deserialize, 
    Serialize,
};
use s3::{
    creds::Credentials,
    region::Region,
};
use url::Url;
use crate::cache::ConfigObjectCache;


/// Configuration params for server
#[derive(Debug, Deserialize, Serialize)]
pub struct Config<'a> {
    pub ident: String,
    pub base_path: Origin<'a>,
    pub cache: Option<ConfigObjectCache>,
    pub creds: Credentials,
    pub connection: ConfigConnection,
}

impl Default for Config<'_> {
    fn default() -> Self {
        Self {
            ident: format!("{}/{}", 
                env!("CARGO_PKG_NAME"), 
                env!("CARGO_PKG_VERSION")
            ),
            base_path: Origin::path_only("/"),
            cache: Default::default(),
            creds: Credentials::anonymous().expect("Error create anonymous AWS credentials"),
            connection: Default::default(),
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
