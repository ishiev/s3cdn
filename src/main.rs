#[macro_use] extern crate rocket;

use std::process;
use rocket::{
    figment::{
        providers::{Env, Format, Serialized, Toml},
        Figment, Profile,
    },
};

mod config;
use config::Config;

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
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
    let config: Config = figment.extract().unwrap_or_else(|err| {
        eprintln!("Problem parsing config: {err}");
        process::exit(1)
    });

    // set server base path from config
    let base_path = config.base_path.to_owned();

    println!("Starting {}, {}",
        env!("CARGO_PKG_DESCRIPTION"),
        config.ident
    );

    rocket::custom(figment)
        .manage(config)
        .mount(base_path, routes![index])
}