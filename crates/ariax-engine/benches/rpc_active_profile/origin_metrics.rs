use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MAX_RESPONSE_BYTES: usize = 16_384;

pub async fn query(address: SocketAddr, pulse: bool) -> Result<Value> {
    let mut phase = "connect";
    tokio::time::timeout(
        Duration::from_secs(5),
        query_inner(address, pulse, &mut phase),
    )
    .await
    .map_err(|_| format!("origin metrics {phase} exceeded five seconds"))?
}

async fn query_inner(address: SocketAddr, pulse: bool, phase: &mut &'static str) -> Result<Value> {
    let mut stream = TcpStream::connect(address).await?;
    *phase = "write";
    stream
        .write_all(if pulse {
            b"GET /pulse HTTP/1.1\r\nHost: localhost\r\n\r\n"
        } else {
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n"
        })
        .await?;
    read_response(&mut stream, phase).await
}

async fn read_response(
    stream: &mut (impl AsyncRead + Unpin),
    phase: &mut &'static str,
) -> Result<Value> {
    *phase = "header read";
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() == MAX_RESPONSE_BYTES {
            return Err("oversized origin metrics response".into());
        }
        header.push(stream.read_u8().await?);
    }
    let text = std::str::from_utf8(&header)?;
    let mut lines = text[..text.len() - 4].split("\r\n");
    if lines.next() != Some("HTTP/1.1 200 OK") {
        return Err("unexpected origin metrics status".into());
    }
    let mut length = None;
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or("malformed origin metrics header")?;
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("unsupported origin metrics transfer encoding".into());
        }
        if name.eq_ignore_ascii_case("content-length") {
            if length.is_some() {
                return Err("duplicate origin metrics content length".into());
            }
            let value = value.trim();
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("invalid origin metrics content length".into());
            }
            length = Some(value.parse::<usize>()?);
        }
    }
    let length = length.ok_or("missing origin metrics content length")?;
    if header
        .len()
        .checked_add(length)
        .is_none_or(|total| total > MAX_RESPONSE_BYTES)
    {
        return Err("oversized origin metrics response".into());
    }
    *phase = "body read";
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await?;
    // Content-Length defines completion. Waiting for TCP EOF can stall after
    // the complete metrics response has already arrived.
    Ok(serde_json::from_slice(&body)?)
}
