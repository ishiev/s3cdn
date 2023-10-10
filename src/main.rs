use std::{
    process, 
    time::Duration, sync::Arc,
};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment, Profile,
};
use axum::{
    routing::get,
    extract::{Path, State, FromRequestParts},
    http::{request::Parts, Uri, StatusCode},
    Router, 
    async_trait, 
    response::IntoResponse,
};
use axum_macros::debug_handler;
use log::{info, debug, error};
use s3::Bucket;
use tokio::signal;

mod config;
mod responder;
mod cache;
mod metacache;
mod housekeeper;
mod error;

use crate::{
    config::Config,
    cache::{DataObject, ObjectKey, ObjectCache, ConditionalHeaders},
    error::Error,
};

const IDENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Application state objects
struct AppState {
    config: Config,
    cache: ObjectCache,
}

/// Fallback handler
async fn fallback(uri: Uri) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, format!("No route for {}", uri))
}

/// Request extractor for conditional headers
#[async_trait]
impl<S> FromRequestParts<S> for ConditionalHeaders
where
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(ConditionalHeaders::from(&parts.headers))
    }
}

#[debug_handler]
async fn index(
    Path(obj): Path<ObjectKey>,
    condition: ConditionalHeaders,
    State(app): State<Arc<AppState>>,
) -> Result<impl IntoResponse, Error> {
    info!("request to object /{} received", obj.cache_key());
    // configure S3 Bucket
    let mut bucket = Bucket::new(
        &obj.bucket, 
        app.config.connection.region.as_ref().unwrap().to_owned(), 
        app.config.creds.to_owned()
    )?; 
    // set timeout from config or infinite
    bucket.set_request_timeout(app.config.connection.timeout.map(Duration::from_secs));
    // set path- or virtual host bucket style 
    if app.config.connection.pathstyle {
        bucket.set_path_style();
    } else {
        bucket.set_subdomain_style();
    }
    // make origin source closure
    let origin = | condition: ConditionalHeaders | {
        // add conditional headers for revalidate requests
        for (key, val) in condition.headers().iter() {
            let Ok(val) = val.to_str() else { continue };
            bucket.add_header(key.as_str(), val);
        }
        // return future to get object from origin with captured context
        let path = obj.key.to_owned();
        async move {
            Ok(DataObject::from(bucket.get_object_stream(path).await?))
        }
    };
    // get object from cache or revalidate
    let object = app.cache.get_object(&obj, origin, condition).await?;
    Ok((app.cache.headers(), object))
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();

    // set configuration sources
    let figment = Figment::from(Serialized::defaults(Config::default()))
        .merge(Toml::file("s3cdn.toml").nested())
        .merge(Toml::file("s3cdn-dev.toml").nested())
        .merge(Env::prefixed("S3CDN").global())
        .select(Profile::from_env_or("S3CDN_PROFILE", "default"));

    // extract the config, exit if error
    let mut config: Config = figment.extract()
        .unwrap_or_else(|err| {
            error!("problem parsing config: {err}");
            process::exit(1)
        });

    // sure for S3 region created
    if config.connection.region.is_none() {
        config.connection.make_custom_region();
    }

    info!("starting {}, {IDENT}", env!("CARGO_PKG_DESCRIPTION")); 

    // setup cache
    let cache = ObjectCache::new(
            config.cache.take().unwrap_or_default(),
            config.housekeeper.take()
        )   
        .unwrap_or_else(|err| {
            error!("cache init error: {err}");
            process::exit(1)
        }
    );

    // make app state
    let app = Arc::new(
        AppState {
            config,
            cache
        }
    );

    // build our application with a route
    let s3_route = Router::new()
        // `GET /` goes to `/<bucket_name>/<key..>`
        .route("/:bucket/*key", get(index))
        .with_state(Arc::clone(&app));

    let app_route = Router::new()
        .nest(app.config.base_path.path(), s3_route)
        .fallback(fallback);

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = app.config.address;
    debug!("listening on {}", addr);
    let res = axum::Server::bind(&addr)
        .serve(app_route.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await;
    // server shutdown procedures
    match res {
        Ok(_) => {
            // save cache and exiting
            app.cache.shutdown().await;
            info!("{} finished", env!("CARGO_PKG_DESCRIPTION"));
        },
        Err(e) => {
            error!("abnormal termination: {}", e);
        }
    }
 }

 async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    // await for signals
    let sig = tokio::select! {
        _ = ctrl_c => { "Ctrl+C" },
        _ = terminate => { "SIGTERM" },
    };
    info!("signal {sig} received, starting graceful shutdown");
}