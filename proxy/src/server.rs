use bytes::BytesMut;
use futures::SinkExt;
use http::{header::HeaderValue, Request, Response, StatusCode};
use native_tls::Identity;
use std::error::Error;
use std::fmt;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

pub(crate) async fn start_plain(listen_addr: &str, beanstalk: &str) -> Result<(), Box<dyn Error>> {
    let server = TcpListener::bind(&listen_addr).await?;
    println!("Listening on: {}", listen_addr);

    loop {
        let (stream, _) = server.accept().await?;
        let beanstalk = beanstalk.to_string();

        tokio::spawn(async move {
            if let Err(e) = process_plain(stream, beanstalk).await {
                println!("failed to process connection; error = {}", e);
            }
        });
    }
}

pub(crate) async fn start_tls(
    listen_addr: &str,
    pkcs12: Identity,
    beanstalk: &str,
) -> Result<(), Box<dyn Error>> {
    let server = TcpListener::bind(&listen_addr).await?;
    println!("Listening on: {}", listen_addr);

    let tls_acceptor =
        tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::builder(pkcs12).build()?);

    loop {
        let (stream, _) = server.accept().await?;
        let tls_stream = tls_acceptor.accept(stream).await.expect("accept error");
        let beanstalk = beanstalk.to_string();
        tokio::spawn(async move {
            if let Err(e) = process_tls(tls_stream, beanstalk).await {
                println!("failed to process connection; error = {}", e);
            }
        });
    }
}

/// Process every request submitted. It will only ever process `Upgrade` requests, everything else will
/// return `501/NOT_IMPLEMENTED`.
async fn process_plain(stream: TcpStream, beanstalk: String) -> Result<(), Box<dyn Error>> {
    let mut transport = Framed::new(stream, Http);
    let mut destination: Option<TcpStream> = None;

    while let Some(request) = transport.next().await {
        match request {
            Ok(request) => {
                let upgraded = upgrade(request, &beanstalk).await?;
                let mut response = Response::builder();

                match upgraded {
                    Some(dst) => {
                        response = response.status(StatusCode::SWITCHING_PROTOCOLS);
                        let response = response
                            .body("".into())
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                        transport.send(response).await?;
                        destination = Some(dst);

                        break;
                    }
                    None => {
                        response = response.status(StatusCode::NOT_IMPLEMENTED);
                        let response = response
                            .body("".into())
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                        transport.send(response).await?;
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    if destination.is_some() {
        proxy_plain(transport.into_inner(), destination.unwrap()).await?;
    }

    Ok(())
}

/// Process every request submitted. It will only ever process `Upgrade` requests, everything else will
/// return `501/NOT_IMPLEMENTED`.
async fn process_tls(
    stream: tokio_native_tls::TlsStream<TcpStream>,
    beanstalk: String,
) -> Result<(), Box<dyn Error>> {
    let mut transport = Framed::new(stream, Http);
    let mut destination: Option<TcpStream> = None;

    while let Some(request) = transport.next().await {
        match request {
            Ok(request) => {
                let upgraded = upgrade(request, &beanstalk).await?;
                let mut response = Response::builder();

                match upgraded {
                    Some(dst) => {
                        response = response.status(StatusCode::SWITCHING_PROTOCOLS);
                        let response = response
                            .body("".into())
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                        transport.send(response).await?;
                        destination = Some(dst);

                        break;
                    }
                    None => {
                        response = response.status(StatusCode::NOT_IMPLEMENTED);
                        let response = response
                            .body("".into())
                            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                        transport.send(response).await?;
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    if destination.is_some() {
        proxy_tls(transport.into_inner(), destination.unwrap()).await?;
    }

    Ok(())
}

/// Whenever the uri `/beanstalkd` is hit with the connection upgrade headers set
/// we will initialize a connection to the configured `beanstalkd` instance and hand
/// back the `TcpStream`
async fn upgrade(req: Request<()>, beanstalk: &str) -> Result<Option<TcpStream>, Box<dyn Error>> {
    match req.uri().path() {
        "/beanstalkd" => {
            let hdr_conn = req.headers().get("Connection");
            let hdr_upgrade = req.headers().get("Upgrade");

            match (hdr_conn, hdr_upgrade) {
                (Some(hdr_conn), Some(hdr_upgrade)) => {
                    let stream = TcpStream::connect(beanstalk).await?;
                    Ok(Some(stream))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Proxies requests between an encrypted `TlsStream` from the client and the destination `TcpStream`
/// which doesn't use TLS. This allows the client-side to use TLS right up to this proxy server
/// which will break the TLS connection and talk to `beanstalkd` over an unencrypted TCP connection.
async fn proxy_plain(source: TcpStream, destination: TcpStream) -> Result<(), Box<dyn Error>> {
    let (mut src_reader, mut src_writer) = source.into_split();
    let (mut dst_reader, mut dst_writer) = destination.into_split();

    let ingress = async {
        tokio::io::copy(&mut src_reader, &mut dst_writer).await?;
        dst_writer.shutdown().await
    };

    let egress = async {
        tokio::io::copy(&mut dst_reader, &mut src_writer).await?;
        src_writer.shutdown().await
    };

    futures::future::try_join(ingress, egress).await?;

    Ok(())
}

/// Proxies requests between an encrypted `TlsStream` from the client and the destination `TcpStream`
/// which doesn't use TLS. This allows the client-side to use TLS right up to this proxy server
/// which will break the TLS connection and talk to `beanstalkd` over an unencrypted TCP connection.
async fn proxy_tls(
    source: tokio_native_tls::TlsStream<TcpStream>,
    destination: TcpStream,
) -> Result<(), Box<dyn Error>> {
    let (mut src_reader, mut src_writer) = tokio::io::split(source);
    let (mut dst_reader, mut dst_writer) = destination.into_split();

    let ingress = async {
        tokio::io::copy(&mut src_reader, &mut dst_writer).await?;
        dst_writer.shutdown().await
    };

    let egress = async {
        tokio::io::copy(&mut dst_reader, &mut src_writer).await?;
        src_writer.shutdown().await
    };

    futures::future::try_join(ingress, egress).await?;

    Ok(())
}

struct Http;

/// Implementation of encoding an HTTP response into a `BytesMut`, basically
/// just writing out an HTTP/1.1 response.
impl Encoder<Response<String>> for Http {
    type Error = io::Error;

    fn encode(&mut self, item: Response<String>, dst: &mut BytesMut) -> io::Result<()> {
        use std::fmt::Write;

        write!(
            BytesWrite(dst),
            "\
             HTTP/1.1 {}\r\n\
             Server: Example\r\n\
             Content-Length: {}\r\n\
             Date: {}\r\n\
             ",
            item.status(),
            item.body().len(),
            time::at(time::get_time()).rfc822(),
        )
        .unwrap();

        for (k, v) in item.headers() {
            dst.extend_from_slice(k.as_str().as_bytes());
            dst.extend_from_slice(b": ");
            dst.extend_from_slice(v.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }

        dst.extend_from_slice(b"\r\n");
        dst.extend_from_slice(item.body().as_bytes());

        return Ok(());

        // Right now `write!` on `Vec<u8>` goes through io::Write and is not
        // super speedy, so inline a less-crufty implementation here which
        // doesn't go through io::Error.
        struct BytesWrite<'a>(&'a mut BytesMut);

        impl fmt::Write for BytesWrite<'_> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.0.extend_from_slice(s.as_bytes());
                Ok(())
            }

            fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> fmt::Result {
                fmt::write(self, args)
            }
        }
    }
}

/// Implementation of decoding an HTTP request from the bytes we've read so far.
/// This leverages the `httparse` crate to do the actual parsing and then we use
/// that information to construct an instance of a `http::Request` object,
/// trying to avoid allocations where possible.
impl Decoder for Http {
    type Item = Request<()>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Request<()>>> {
        // TODO: we should grow this headers array if parsing fails and asks
        //       for more headers
        let mut headers = [None; 16];
        let (method, path, version, amt) = {
            let mut parsed_headers = [httparse::EMPTY_HEADER; 16];
            let mut r = httparse::Request::new(&mut parsed_headers);

            let status = r.parse(src).map_err(|e| {
                let msg = format!("failed to parse http request: {:?}", e);
                io::Error::new(io::ErrorKind::Other, msg)
            })?;

            let amt = match status {
                httparse::Status::Complete(amt) => amt,
                httparse::Status::Partial => return Ok(None),
            };

            let toslice = |a: &[u8]| {
                let start = a.as_ptr() as usize - src.as_ptr() as usize;
                assert!(start < src.len());
                (start, start + a.len())
            };

            for (i, header) in r.headers.iter().enumerate() {
                let k = toslice(header.name.as_bytes());
                let v = toslice(header.value);
                headers[i] = Some((k, v));
            }

            (
                toslice(r.method.unwrap().as_bytes()),
                toslice(r.path.unwrap().as_bytes()),
                r.version.unwrap(),
                amt,
            )
        };

        if version != 1 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "only HTTP/1.1 accepted",
            ));
        }

        let data = src.split_to(amt).freeze();
        let mut ret = Request::builder();
        ret = ret.method(&data[method.0..method.1]);
        let s = data.slice(path.0..path.1);
        let s = unsafe { String::from_utf8_unchecked(Vec::from(s.as_ref())) };
        ret = ret.uri(s);
        ret = ret.version(http::Version::HTTP_11);
        for header in headers.iter() {
            let (k, v) = match *header {
                Some((ref k, ref v)) => (k, v),
                None => break,
            };
            let value = HeaderValue::from_bytes(data.slice(v.0..v.1).as_ref())
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "header decode error"))?;
            ret = ret.header(&data[k.0..k.1], value);
        }

        let req = ret
            .body(())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Some(req))
    }
}
