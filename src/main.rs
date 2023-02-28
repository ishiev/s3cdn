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
    Request
};
mod config;
use config::Config;
mod responder;
use responder::DataStreamResponder;
use s3::{
    Bucket,
    error::S3Error,
};

#[derive(Responder)]
enum Error {
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

#[get("/<bucket_name>/<path..>")]
async fn index(
    bucket_name: &str,
    path: PathBuf,
    config: &State<Config<'_>>,
) -> Result<DataStreamResponder, Error> {
    // configure S3 Bucket
    let bucket = {
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
        bucket
    };

    let response = bucket.get_object_stream(path.to_string_lossy()).await?;

    Ok(DataStreamResponder::from(response))
}

#[launch]
fn rocket() -> _ {
    // set configutation sources
    let figment = Figment::from(rocket::Config::default())
        .merge(Serialized::defaults(Config::default()))
        .merge(Toml::file("s3cdn.toml").nested())
        .merge(Env::prefixed("S3CDN").global())
        .select(Profile::from_env_or("S3CDN_PROFILE", "default"));

    // extract the config, exit if error
    let mut config: Config = figment.extract().unwrap_or_else(|err| {
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

    // set server base path from config
    let base_path = config.base_path.to_owned();

    println!("Starting {}, {}",
        env!("CARGO_PKG_DESCRIPTION"),
        config.ident
    );

    println!("Config: {:?}", config);

    rocket::custom(figment)
        .manage(config)
        .mount(base_path, routes![index])
        .register("/", catchers![default_catcher])
}