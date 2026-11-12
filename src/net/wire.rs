//! Framing of messages on a stream, and the stream header.

use std::{io, time::Duration};

use bytes::BytesMut;
use iroh::endpoint::{RecvStream, SendStream};
use n0_error::{e, stack_error};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::AsyncReadExt;

use crate::proto::TopicId;

/// How long a write may make no progress before it fails the connection.
///
/// A peer extends its flow control window only after it read part of it, so
/// the writes to a slow reader block for a while now and then.
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(if cfg!(test) { 5 } else { 60 });

/// How long opening a stream may wait at the stream limit of the peer before it fails the connection.
///
/// The peer raises its limit only once one of our streams ends. With as many
/// topics as the limit, the wait does not end. The limit is the
/// `max_concurrent_uni_streams` of the peer's endpoint, 100 by default. A
/// stalled open of an initial topic only skips the other initial topics.
pub(super) const OPEN_TIMEOUT: Duration = Duration::from_secs(if cfg!(test) { 5 } else { 15 });

/// The flag of [`StreamHeader::handles_close`].
const HANDLES_CLOSE: u8 = 1;

/// Errors related to message writing
#[allow(missing_docs)]
#[stack_error(derive, add_meta, from_sources)]
#[non_exhaustive]
pub(super) enum WriteError {
    /// Connection error
    #[error("Connection error")]
    Connection {
        #[error(std_err)]
        source: iroh::endpoint::ConnectionError,
    },
    /// Serialization failed
    #[error("Serialization failed")]
    Ser {
        #[error(std_err)]
        source: postcard::Error,
    },
    /// IO error
    #[error("IO error")]
    Io {
        #[error(std_err)]
        source: std::io::Error,
    },
    /// Message was larger than the configured maximum message size
    #[error("message too large")]
    TooLarge {},
    /// A write made no progress for [`WRITE_TIMEOUT`].
    #[error("no write progress for {WRITE_TIMEOUT:?}")]
    Stalled {},
    /// Opening a stream waited for [`OPEN_TIMEOUT`] at the stream limit of the peer.
    #[error("stream limit of the peer reached for {OPEN_TIMEOUT:?}")]
    OpenStalled {},
}

/// The first frame of a stream: the topic of its messages, and a byte of flags.
///
/// Nodes up to 0.101 send the topic alone, and ignore the flags when they read
/// a header, as they ignore the rest of a frame.
#[derive(Debug)]
pub(super) struct StreamHeader {
    pub(super) topic: TopicId,
    /// Whether the sender drops its state for a peer whose connection closes.
    ///
    /// A node without it loses every later message to a peer that closes a
    /// connection that is no neighbor link of the node. So we close no
    /// connection to a peer that did not send it.
    pub(super) handles_close: bool,
}

impl StreamHeader {
    pub(super) async fn read(
        stream: &mut RecvStream,
        buffer: &mut BytesMut,
        max_message_size: usize,
    ) -> Result<Self, ReadError> {
        let frame = read_lp(stream, buffer, max_message_size)
            .await?
            .ok_or_else(|| {
                ReadError::from(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended before header",
                ))
            })?;
        Self::decode(frame)
    }

    fn decode(frame: &[u8]) -> Result<Self, ReadError> {
        let (topic, rest) = postcard::take_from_bytes::<TopicId>(frame)?;
        Ok(Self {
            topic,
            handles_close: rest.first().is_some_and(|flags| flags & HANDLES_CLOSE != 0),
        })
    }

    /// Writes the header that we send on a stream for `topic`.
    ///
    /// We always set [`Self::handles_close`].
    pub(super) async fn write(
        topic: TopicId,
        stream: &mut SendStream,
        buffer: &mut Vec<u8>,
        max_message_size: usize,
    ) -> Result<(), WriteError> {
        write_message(stream, &(topic, HANDLES_CLOSE), buffer, max_message_size).await
    }
}

/// Errors related to message reading
#[allow(missing_docs)]
#[stack_error(derive, add_meta, from_sources)]
#[non_exhaustive]
pub(super) enum ReadError {
    /// Deserialization failed
    #[error("Deserialization failed")]
    De {
        #[error(std_err)]
        source: postcard::Error,
    },
    /// IO error
    #[error("IO error")]
    Io {
        #[error(std_err)]
        source: std::io::Error,
    },
    /// Message was larger than the configured maximum message size
    #[error("message too large")]
    TooLarge {},
}

/// Read a length-prefixed frame and decode with postcard.
pub(super) async fn read_message<T: DeserializeOwned>(
    reader: &mut RecvStream,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> Result<Option<T>, ReadError> {
    let Some(data) = read_lp(reader, buffer, max_message_size).await? else {
        return Ok(None);
    };
    Ok(Some(postcard::from_bytes(data)?))
}

/// Reads a length prefixed buffer.
///
/// Borrows the frame from `buffer`, keeping its storage for the next read.
/// Returns `None` if the stream ends before the frame length starts.
pub(super) async fn read_lp<'a>(
    reader: &mut RecvStream,
    buffer: &'a mut BytesMut,
    max_message_size: usize,
) -> Result<Option<&'a [u8]>, ReadError> {
    let size = match reader.read_u32().await {
        Ok(size) => size,
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let size = usize::try_from(size).map_err(|_| e!(ReadError::TooLarge))?;
    if size > max_message_size {
        return Err(e!(ReadError::TooLarge));
    }
    // The buffer grows with the bytes that arrive. Sizing it to the declared length up
    // front would let a peer that declares `max_message_size` on every stream and sends
    // nothing more hold that much memory per stream.
    buffer.clear();
    while buffer.len() < size {
        let chunk = reader
            .read_chunk(size - buffer.len())
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        buffer.extend_from_slice(&chunk);
    }
    Ok(Some(&buffer[..]))
}

/// Writes a length-prefixed frame.
pub(super) async fn write_message<T: Serialize>(
    stream: &mut SendStream,
    message: &T,
    buffer: &mut Vec<u8>,
    max_message_size: usize,
) -> Result<(), WriteError> {
    let len = postcard::experimental::serialized_size(&message)?;
    write_sized_message(stream, message, len, buffer, max_message_size).await
}

/// Writes a length-prefixed frame of `message`, whose encoded size is `len`.
pub(super) async fn write_sized_message<T: Serialize>(
    stream: &mut SendStream,
    message: &T,
    len: usize,
    buffer: &mut Vec<u8>,
    max_message_size: usize,
) -> Result<(), WriteError> {
    // A frame must be shorter than the limit. Nodes up to 0.101 read a frame of
    // the limit, but refuse to write one, so they could not forward it.
    if len >= max_message_size {
        return Err(e!(WriteError::TooLarge));
    }
    buffer.clear();
    buffer.extend_from_slice(&(len as u32).to_be_bytes());
    buffer.resize(4 + len, 0u8);
    postcard::to_slice(&message, &mut buffer[4..])?;
    // Each write takes some bytes, or fails once the peer takes none for long.
    // A slow peer thus keeps the connection, however long the frame takes.
    let mut sent = 0;
    while sent < buffer.len() {
        sent += n0_future::time::timeout(WRITE_TIMEOUT, stream.write(&buffer[sent..]))
            .await
            .map_err(|_| e!(WriteError::Stalled))?
            .map_err(io::Error::other)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use iroh::{
        endpoint::{presets, Connection},
        Endpoint, EndpointAddr, RelayMode, TransportAddr,
    };
    use serde::Deserialize;
    use tokio::io::AsyncWriteExt;

    use super::*;

    const TEST_ALPN: &[u8] = b"iroh-gossip/test/wire";

    async fn loopback_endpoint() -> Endpoint {
        Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![TEST_ALPN.to_vec()])
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")
            .expect("valid bind address")
            .bind()
            .await
            .expect("bind")
    }

    /// A connection from a new endpoint to another, accepted on the other side.
    async fn connected_pair() -> (Endpoint, Endpoint, Connection, Connection) {
        let a = loopback_endpoint().await;
        let b = loopback_endpoint().await;
        let addr = EndpointAddr {
            id: b.id(),
            addrs: b
                .bound_sockets()
                .into_iter()
                .map(TransportAddr::Ip)
                .collect(),
        };
        let (dialed, accepted) = tokio::join!(a.connect(addr, TEST_ALPN), async {
            b.accept().await.expect("incoming").await.expect("accept")
        });
        (a, b, dialed.expect("connect"), accepted)
    }

    /// A frame that arrives in several writes is read whole.
    #[tokio::test]
    async fn read_lp_reads_a_frame_sent_in_pieces() {
        let (_a, _b, dialed, accepted) = connected_pair().await;
        let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let mut send = dialed.open_uni().await.expect("open");
        send.write_u32(payload.len() as u32).await.expect("write");
        for piece in payload.chunks(7_000) {
            send.write_all(piece).await.expect("write");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        send.finish().expect("finish");

        let mut recv = accepted.accept_uni().await.expect("accept");
        let mut buffer = BytesMut::new();
        let frame = read_lp(&mut recv, &mut buffer, 64 * 1024)
            .await
            .expect("read")
            .expect("frame");
        assert_eq!(frame, &payload[..]);
        assert_eq!(
            read_lp(&mut recv, &mut buffer, 64 * 1024)
                .await
                .expect("read"),
            None
        );
    }

    /// A frame whose declared length is not sent holds only the bytes that arrived.
    #[tokio::test]
    async fn read_lp_does_not_allocate_the_declared_length() {
        let (_a, _b, dialed, accepted) = connected_pair().await;
        let mut send = dialed.open_uni().await.expect("open");
        send.write_u32(60_000).await.expect("write");
        send.write_all(&[1; 10]).await.expect("write");

        let mut recv = accepted.accept_uni().await.expect("accept");
        let mut buffer = BytesMut::new();
        let read = tokio::time::timeout(
            Duration::from_millis(200),
            read_lp(&mut recv, &mut buffer, 64 * 1024),
        )
        .await;
        assert!(read.is_err(), "the frame is incomplete");
        assert!(buffer.capacity() < 60_000, "capacity {}", buffer.capacity());
    }

    /// The header as nodes up to 0.101 encode and decode it.
    #[derive(Debug, Serialize, Deserialize)]
    struct OldStreamHeader {
        topic_id: TopicId,
    }

    /// Old and new nodes read the stream headers of each other.
    #[test]
    fn stream_header_works_across_versions() {
        let topic_id: TopicId = [7; 32].into();
        let new = postcard::to_stdvec(&(topic_id, HANDLES_CLOSE)).expect("encode");
        let old = postcard::to_stdvec(&OldStreamHeader { topic_id }).expect("encode");

        let read_by_old: OldStreamHeader = postcard::from_bytes(&new).expect("old reads new");
        assert_eq!(read_by_old.topic_id, topic_id);

        let header = StreamHeader::decode(&old).expect("new reads old");
        assert_eq!(header.topic, topic_id);
        assert!(!header.handles_close);

        let header = StreamHeader::decode(&new).expect("new reads new");
        assert_eq!(header.topic, topic_id);
        assert!(header.handles_close);
    }
}
