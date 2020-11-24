use http::{header, Request, Version};
use std::error::Error;
use std::io::Write;
use tokio::io;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(crate) async fn write_request<W: AsyncWrite + std::marker::Unpin>(
    writer: &mut W,
    req: &Request<String>,
) -> Result<(), Box<dyn Error>> {
    let version_str = match req.version() {
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        v => panic!("Unsupported version: {:?}", v),
    };

    let mut data = vec![];

    write!(
        &mut data,
        "{} {} {}\r\n",
        req.method(),
        req.uri(),
        version_str
    )?;
    write!(&mut data, "{}: {}\r\n", header::HOST.as_str(), "empty")?;
    for (name, value) in req.headers().iter() {
        write!(&mut data, "{}: {}\r\n", name.as_str(), value.to_str()?)?;
    }

    if req.body().len() > 0 {
        write!(&mut data, "content-length: {}\r\n", req.body().len())?;
    }

    write!(&mut data, "\r\n{}", req.body())?;
    //println!("{}", String::from_utf8(data).unwrap());
    writer.write(&data).await;
    Ok(())
}
