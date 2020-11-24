use beanstalk_auth_tls::async_impl::beanstalkc::Beanstalkc;
use std::error::Error;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut cl = Beanstalkc::new()
        .host("10.20.1.87")
        .port(443)
        .connect_tls(true)
        .await?;

    let x = beanstalk_auth_tls::http::Request::builder()
        .uri("/api/login")
        .method("POST")
        //  .header("Connection", "Upgrade")
        //  .header("Upgrade", "Beanstalkd")
        .header("Content-Type", "application/json;charset=utf-8")
        .body(
            "{ \"emailAddress\": \"admin@farsight.rs\", \"password\": \"Portugal.01\" }"
                .to_string(),
        )
        .unwrap();

    let resp = cl.http_req_tls(x).await?;
    println!("{}", resp.body());

    let tubes = cl.tubes().await?;

    println!("{:?}", tubes);
    Ok(())
}
