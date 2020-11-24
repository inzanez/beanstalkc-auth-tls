use std::error::Error;
use beanstalk_auth_tls::async_impl::beanstalkc::Beanstalkc;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut cl = Beanstalkc::new().host("10.41.41.240").port(11300).connect().await?;

    let tubes = cl.tubes().await?;

    println!("{:?}", tubes);
    Ok(())
}