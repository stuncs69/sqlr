use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor, Read};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::mysql::error::{MySQLError, Result};

const MAX_PACKET_SIZE: usize = (1 << 24) - 1;

#[derive(Debug, Clone)]
pub struct Packet {
    pub sequence_id: u8,
    pub payload: Bytes,
}

impl Packet {
    pub fn new(sequence_id: u8, payload: Bytes) -> Self {
        Packet {
            sequence_id,
            payload,
        }
    }

    pub async fn read_from<R>(reader: &mut R) -> Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0u8; 4];
        reader.read_exact(&mut header).await?;

        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let sequence_id = header[3];

        if payload_len > MAX_PACKET_SIZE {
            return Err(MySQLError::Protocol(format!(
                "Packet size {} exceeds maximum allowed size {}",
                payload_len, MAX_PACKET_SIZE
            )));
        }

        let mut payload = BytesMut::with_capacity(payload_len);
        payload.resize(payload_len, 0);
        reader.read_exact(&mut payload).await?;

        Ok(Packet {
            sequence_id,
            payload: payload.freeze(),
        })
    }

    pub async fn write_to<W>(&self, writer: &mut W) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let payload_len = self.payload.len();
        if payload_len > MAX_PACKET_SIZE {
            return Err(MySQLError::Protocol(format!(
                "Packet size {} exceeds maximum allowed size {}",
                payload_len, MAX_PACKET_SIZE
            )));
        }

        let header = [
            (payload_len & 0xFF) as u8,
            ((payload_len >> 8) & 0xFF) as u8,
            ((payload_len >> 16) & 0xFF) as u8,
            self.sequence_id,
        ];

        writer.write_all(&header).await?;
        writer.write_all(&self.payload).await?;
        writer.flush().await?;

        Ok(())
    }

    pub fn cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(&self.payload[..])
    }
}

pub struct PacketReader<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> PacketReader<'a> {
    pub fn new(packet: &'a Packet) -> Self {
        Self {
            cursor: packet.cursor(),
        }
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        Read::read_exact(&mut self.cursor, &mut buf)?;
        Ok(buf[0])
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        ReadBytesExt::read_u16::<LittleEndian>(&mut self.cursor).map_err(MySQLError::from)
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        ReadBytesExt::read_u32::<LittleEndian>(&mut self.cursor).map_err(MySQLError::from)
    }

    pub fn read_lenenc_int(&mut self) -> Result<u64> {
        let first_byte = self.read_u8()?;

        match first_byte {
            0xFB => Ok(0xFB),
            0xFC => {
                let val = ReadBytesExt::read_u16::<LittleEndian>(&mut self.cursor)?;
                Ok(u64::from(val))
            }
            0xFD => {
                let mut buf = [0u8; 4];
                Read::read_exact(&mut self.cursor, &mut buf[0..3])?;
                Ok(u64::from(u32::from_le_bytes([buf[0], buf[1], buf[2], 0])))
            }
            0xFE => {
                ReadBytesExt::read_u64::<LittleEndian>(&mut self.cursor).map_err(MySQLError::from)
            }
            _ => Ok(u64::from(first_byte)),
        }
    }

    pub fn read_lenenc_string(&mut self) -> Result<String> {
        let len = self.read_lenenc_int()? as usize;
        let mut buf = vec![0u8; len];
        Read::read_exact(&mut self.cursor, &mut buf)?;

        String::from_utf8(buf).map_err(|e| MySQLError::Protocol(format!("Invalid UTF-8: {}", e)))
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        Read::read_exact(&mut self.cursor, &mut buf)?;
        Ok(buf)
    }

    pub fn read_null_terminated_string(&mut self) -> Result<String> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            Read::read_exact(&mut self.cursor, &mut byte)?;
            if byte[0] == 0 {
                break;
            }
            bytes.push(byte[0]);
        }

        String::from_utf8(bytes).map_err(|e| MySQLError::Protocol(format!("Invalid UTF-8: {}", e)))
    }

    pub fn remaining(&self) -> usize {
        self.cursor.remaining()
    }
}

pub struct PacketBuilder {
    buffer: BytesMut,
}

impl PacketBuilder {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(64),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    pub fn write_u8(&mut self, val: u8) -> &mut Self {
        self.buffer.put_u8(val);
        self
    }

    pub fn write_u16(&mut self, val: u16) -> &mut Self {
        self.buffer.put_u16_le(val);
        self
    }

    pub fn write_u32(&mut self, val: u32) -> &mut Self {
        self.buffer.put_u32_le(val);
        self
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.buffer.put_slice(bytes);
        self
    }

    pub fn write_lenenc_int(&mut self, val: u64) -> &mut Self {
        if val < 251 {
            self.buffer.put_u8(val as u8);
        } else if val < 65536 {
            self.buffer.put_u8(0xFC);
            self.buffer.put_u16_le(val as u16);
        } else if val < 16777216 {
            self.buffer.put_u8(0xFD);
            self.buffer.put_u8((val & 0xFF) as u8);
            self.buffer.put_u8(((val >> 8) & 0xFF) as u8);
            self.buffer.put_u8(((val >> 16) & 0xFF) as u8);
        } else {
            self.buffer.put_u8(0xFE);
            self.buffer.put_u64_le(val);
        }
        self
    }

    pub fn write_lenenc_string(&mut self, s: &str) -> &mut Self {
        self.write_lenenc_int(s.len() as u64);
        self.buffer.put_slice(s.as_bytes());
        self
    }

    pub fn write_null_terminated_string(&mut self, s: &str) -> &mut Self {
        self.buffer.put_slice(s.as_bytes());
        self.buffer.put_u8(0);
        self
    }

    pub fn build(self, sequence_id: u8) -> Packet {
        Packet {
            sequence_id,
            payload: self.buffer.freeze(),
        }
    }
}
