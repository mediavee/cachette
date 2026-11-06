//! A raw HTTP/1.1 client for tests that need to see the bytes: interim `100` responses, a
//! connection that stays open or closes, a body written in pieces.

use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);

/// One response: interim (`100`) and final alike.
#[derive(Debug)]
pub(crate) struct Reply {
    pub(crate) status: u16,
    /// Header names lower-cased, values as sent.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Reply {
    /// The first header named `name` (lower case).
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every value of the header named `name` (lower case).
    pub(crate) fn headers_named(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
            .collect()
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// Whether the server announced that it will close the connection.
    pub(crate) fn closes(&self) -> bool {
        self.header("connection")
            .is_some_and(|value| value.eq_ignore_ascii_case("close"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// A body of `Content-Length` bytes follows the head.
    Body,
    /// The response answers a `HEAD`: no body follows whatever the head says.
    HeadOnly,
}

/// A connection kept open across requests.
#[derive(Debug)]
pub(crate) struct Wire {
    stream: TcpStream,
    pending: Vec<u8>,
}

impl Wire {
    pub(crate) fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect_timeout(&addr, TIMEOUT).expect("connect");
        stream
            .set_read_timeout(Some(TIMEOUT))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(TIMEOUT))
            .expect("write timeout");
        stream.set_nodelay(true).expect("nodelay");
        Self {
            stream,
            pending: Vec::new(),
        }
    }

    /// Writes raw bytes.
    pub(crate) fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes)
    }

    /// Writes a request head: the request line, `Host`, `headers`, and the blank line.
    pub(crate) fn send_head(&mut self, method: &str, path: &str, headers: &[(&str, &str)]) {
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: cachette\r\n");
        for (name, value) in headers {
            write!(head, "{name}: {value}\r\n").expect("write to a String");
        }
        head.push_str("\r\n");
        self.send(head.as_bytes()).expect("send request head");
    }

    /// A complete request with a `Content-Length` body, answered by the next final response.
    pub(crate) fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Reply {
        let length = body.len().to_string();
        let mut all = headers.to_vec();
        all.push(("Content-Length", &length));
        self.send_head(method, path, &all);
        self.send(body).expect("send request body");
        self.read_reply().expect("read response")
    }

    /// The next response, whose body has `Content-Length` bytes.
    ///
    /// # Errors
    ///
    /// Fails when the peer closes or stalls before a whole response arrived.
    pub(crate) fn read_reply(&mut self) -> io::Result<Reply> {
        self.read_reply_with(Framing::Body)
    }

    /// The next response to a `HEAD`: headers only, whatever `Content-Length` says.
    pub(crate) fn read_head_reply(&mut self) -> io::Result<Reply> {
        self.read_reply_with(Framing::HeadOnly)
    }

    fn read_reply_with(&mut self, framing: Framing) -> io::Result<Reply> {
        let end = loop {
            if let Some(end) = find(&self.pending, b"\r\n\r\n") {
                break end;
            }
            self.fill()?;
        };
        let head = String::from_utf8_lossy(&self.pending[..end]).into_owned();
        self.pending.drain(..end + 4);
        let mut lines = head.lines();
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no status line"))?;
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        let mut reply = Reply {
            status,
            headers,
            body: Vec::new(),
        };
        if framing == Framing::HeadOnly || status < 200 || status == 204 || status == 304 {
            return Ok(reply);
        }
        let length: usize = reply
            .header("content-length")
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no Content-Length"))?;
        while self.pending.len() < length {
            self.fill()?;
        }
        reply.body = self.pending.drain(..length).collect();
        Ok(reply)
    }

    fn fill(&mut self) -> io::Result<()> {
        let mut chunk = vec![0_u8; 256 * 1024];
        let read = self.stream.read(&mut chunk)?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        self.pending.extend_from_slice(&chunk[..read]);
        Ok(())
    }

    /// Reads up to `limit` bytes of the body of a response already announced by its head, for a
    /// test that reads slowly.
    pub(crate) fn read_some(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        if self.pending.is_empty() {
            self.fill()?;
        }
        let take = limit.min(self.pending.len());
        Ok(self.pending.drain(..take).collect())
    }

    /// Whether the server has closed its side. Waits at most `within` for it to.
    pub(crate) fn is_closed(&mut self, within: Duration) -> bool {
        self.stream
            .set_read_timeout(Some(within))
            .expect("read timeout");
        let mut byte = [0_u8; 1];
        let closed = match self.stream.read(&mut byte) {
            Ok(0) => true,
            Ok(_) => panic!("unexpected data on an idle connection"),
            Err(e) => matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ),
        };
        self.stream
            .set_read_timeout(Some(TIMEOUT))
            .expect("read timeout");
        closed
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
