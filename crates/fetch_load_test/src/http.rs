// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Small HTTP/1.1 control client used outside measured Fetch traffic.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

/// One raw and parsed control-plane HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Decoded response body.
    pub body: Vec<u8>,
    /// Exact bytes read from the socket, including headers and framing.
    pub raw: Vec<u8>,
}

/// Sends one loopback HTTP request and returns the status and body.
///
/// # Errors
///
/// Returns an error when the socket, HTTP response, or chunk framing is invalid.
pub fn request(port: u16, method: &str, path: &str) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )?;
    stream.flush()?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    response_from_raw(response)
}

fn response_from_raw(response: Vec<u8>) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
    let (status, body) = parse_response(&response)?;
    Ok(Response {
        status,
        body,
        raw: response,
    })
}

fn parse_response(response: &[u8]) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("HTTP response has no header boundary")?;
    let header_bytes = &response[..boundary];
    let headers = std::str::from_utf8(header_bytes)?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("HTTP response has no status")?
        .parse::<u16>()?;
    let body = &response[boundary + 4..];

    if headers.lines().any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked")) {
        return Ok((status, decode_chunked(body)?));
    }

    Ok((status, body.to_vec()))
}

fn decode_chunked(mut body: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or("chunk size has no terminator")?;
        let size_text = std::str::from_utf8(&body[..line_end])?;
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or_default(), 16)?;
        body = &body[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if body.len() < size + 2 || &body[size..size + 2] != b"\r\n" {
            return Err("chunk body is incomplete".into());
        }
        decoded.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
}
