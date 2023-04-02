#[macro_use] extern crate rocket;

use std::{
    process, 
    path::PathBuf, 
    time::Duration,
};
use rocket::{
    figment::{
        providers::{Env, Format, Serialized, Toml},
        Figment, Profile,
    }, 
    State,
    http::Status,
    Request, request::{FromRequest, Outcome},
};
use s3::{
    Bucket,
    error::S3Error,
};

mod config;
mod responder;
mod cache;
mod metacache;

use crate::{
    config::Config,
    responder::CacheResponder,
    cache::{DataObject, ObjectKey, ObjectCache, ConditionalHeaders},
};


#[derive(Responder)]
enum Error {
    #[response(status = 304)]
    NotModified(String),
    #[response(status = 404)]
    NotFound(String),
    #[response(status = 403)]
    Forbidden(String),
    #[response(status = 503)]
    Unavailable(String),
    #[response(status = 500)]
    Internal(String)
}

impl From<S3Error> for Error {
    fn from(e: S3Error) -> Self {
        match e {
            S3Error::Http(304, e)   => Error::NotModified(e),
            S3Error::Http(404, _)   => Error::NotFound(e.to_string()),
            S3Error::Http(403, _)   => Error::Forbidden(e.to_string()),
            S3Error::Credentials(_) => Error::Forbidden(e.to_string()),
            S3Error::MaxExpiry(_)   => Error::Unavailable(e.to_string()),
            S3Error::HttpFail       => Error::Unavailable(e.to_string()),
            S3Error::Reqwest(e)     => Error::Unavailable(e.to_string()),
            _ => Error::Internal(e.to_string())
        }
    }
}

#[catch(default)]
fn default_catcher(status: Status, _: &Request) -> String {
    format!("{}", status)
}

/// Request guard for conditional headers
#[rocket::async_trait]
impl<'r> FromRequest<'r> for ConditionalHeaders<'r> {
    type Error = std::convert::Infallible;

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        Outcome::Success(
            ConditionalHeaders::from(req.headers())
        )
    }
}

#[get("/<bucket_name>/<path..>")]
async fn index<'r>(
    bucket_name: &'r str,
    path: PathBuf,
    condition: ConditionalHeaders<'_>,
    config: &State<Config<'_>>,
    cache: &'r State<ObjectCache>,
) -> Result<CacheResponder<'r, 'static, DataObject>, Error> {
    // configure S3 Bucket
    let mut bucket = Bucket::new(
        bucket_name, 
        config.connection.region.as_ref().unwrap().to_owned(), 
        config.creds.to_owned()
    )?;
    
    // set timeout from config or infinite
    bucket.set_request_timeout(config.connection.timeout.map(Duration::from_secs));
    // set path- or virtual host bucket style 
    if config.connection.pathstyle {
        bucket.set_path_style();
    } else {
        bucket.set_subdomain_style();
    }
      
    // make origin source closure
    let origin = | condition: ConditionalHeaders | {
        // add conditional headers for revalidate requests
        for h in condition.headers().into_iter() {
            bucket.add_header(h.name().as_str(), h.value());
        }
        // construct object path string
        let path = path.to_string_lossy();
        // return future to get object from origin with captured context
        async move {
            Ok(DataObject::from(bucket.get_object_stream(path).await?))
        }
    };

    // make object key for cache request
    let key = ObjectKey {
        bucket: bucket_name,
        key: &path,
    };
    // get object from cache or revalidate
    let object = cache.get_object(key, origin, condition).await?;
    
    Ok(CacheResponder::new(
        object,
        cache
    ))
}

#[rocket::main]
async fn main() {
    // set configutation sources
    let figment = Figment::from(rocket::Config::default())
        .merge(Serialized::defaults(Config::default()))
        .merge(Toml::file("s3cdn.toml").nested())
        .merge(Toml::file("s3cdn-dev.toml").nested())
        .merge(Env::prefixed("S3CDN").global())
        .select(Profile::from_env_or("S3CDN_PROFILE", "default"));

    // extract the config, exit if error
    let mut config: Config = figment.extract()
        .unwrap_or_else(|err| {
            eprintln!("Problem parsing config: {err}");
            process::exit(1)
        });

    // sure for S3 region created
    if config.connection.region.is_none() {
        if config.connection.endpoint.is_none() {
            eprintln!("S3 connection endpoint not found in config, set \"config.connection.endpoint\" param!");
            process::exit(1)
        } else {
            config.connection.make_custom_region();
        }
    }

    // setup cache
    let cache = ObjectCache::new(config.cache.take().unwrap_or_default())
        .unwrap_or_else(|err| {
            eprintln!("Init error: {err}");
            process::exit(1)
        });


    println!("Starting {}, {}",
        env!("CARGO_PKG_DESCRIPTION"),
        config.ident
    );    

    // set server base path from config
    let base_path = config.base_path.to_owned();

    let res = rocket::custom(figment)
        .manage(cache)
        .manage(config)
        .mount(base_path, routes![index])
        .register("/", catchers![default_catcher])
        .launch()
        .await;

    match res {
        Ok(res) => {
            if let Some(cache) = res.state::<ObjectCache>() {
                // save cache and exiting
                cache.shutdown().await;
                info!("{} terminated.", env!("CARGO_PKG_DESCRIPTION"));
            } 
        },
        Err(e) => {
            error!("Abnormal termination: {}", e);
        }
    }
 }