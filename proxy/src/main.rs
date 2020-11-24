#![warn(rust_2018_idioms)]

use std::error::Error;
use clap::{App, Arg};
use std::fs::File;
use std::io::Read;
use native_tls::Identity;

pub(crate) mod server;

const DEFAULT_LISTEN: &str = "0.0.0.0:80";
const DEFAULT_LISTEN_TLS: &str = "0.0.0.0:443";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let matches = App::new("Beanstalkd Proxy")
        .version("0.1.0")
        .author("Inzanez <r.aggeler@gmx.net>")
        .about("Upgrades HTTP(s) connections to bidirectional proxied TCP connection")
        .arg(Arg::new("beanstalkd")
            .short('b')
            .long("beanstalkd")
            .value_name("Beanstalkd endpoint")
            .about("Defines the beanstalkd address and port to proxy to")
            .required(true)
            .takes_value(true))
        .arg(Arg::new("listen")
            .short('l')
            .long("listen")
            .value_name("Listen address")
            .about("Sets the input file to use")
            .takes_value(true))
        .arg(Arg::new("identity")
            .short('i')
            .long("identity")
            .value_name("P12 file for TLS")
            .about("Sets an identity and starts the server in TLS mode"))
        .get_matches();

    println!("{:?}", matches.value_of("identity"));

    let identity = matches.value_of("identity");
    let listen = matches.value_of("listen").unwrap_or_else(|| {
        if identity.is_none() {
            DEFAULT_LISTEN
        } else {
            DEFAULT_LISTEN_TLS
        }
    });
    let beanstalkd = matches.value_of("beanstalkd").expect("Could not retrieve beanstalk endpoint");

    if identity.is_none() {
        server::start_plain(listen, beanstalkd).await?;
    } else {
        let identity = identity.unwrap();
        let mut file = File::open(identity).expect("Could not read identity file");
        let mut pkcs12 = vec![];
        file.read_to_end(&mut pkcs12).unwrap();
        let pkcs12 = Identity::from_pkcs12(&pkcs12, "hunter2").unwrap();

        server::start_tls(listen, pkcs12, beanstalkd).await?;
    }

    Ok(())
}
