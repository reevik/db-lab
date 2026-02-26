use crate::errors::InvalidPageOffsetError;
use core::fmt::Debug;
use std::cmp::min;
use std::io::Read;
use std::ops::{Add, Mul, Sub};

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Hash, Ord)]
pub(crate) struct OffsetType<T>(pub T);

impl OffsetType<u16> {

    pub(crate) fn get(&self,) -> usize {
        self.0 as usize
    }

    pub(crate) fn from_u16(i: u16) -> Self {
        OffsetType(i)
    }
}

/// o16 is a type alias representing the offset type in a page.
pub(crate) type o16 = OffsetType<u16>;

// A convenience function to create o16 types from u16.
pub(crate) const fn o16(value: u16) -> o16 {
    OffsetType(value)
}

impl<T> TryFrom<usize> for OffsetType<T>
where
    T: TryFrom<usize>,
{
    type Error = InvalidPageOffsetError;

    fn try_from(value: usize) -> Result<Self, InvalidPageOffsetError> {
        T::try_from(value)
            .map(OffsetType)
            .map_err(|_| InvalidPageOffsetError::OutOfRange)
    }
}

impl<T> TryFrom<OffsetType<T>> for usize
where
    usize: TryFrom<T>,
{
    type Error = InvalidPageOffsetError;

    fn try_from(value: OffsetType<T>) -> Result<Self, InvalidPageOffsetError> {
        if let Ok(ok) = usize::try_from(value.0) {
            return Ok(ok);
        }
        Err(InvalidPageOffsetError::OutOfRange)
    }
}


impl<T> Sub for OffsetType<T>
where
    T: Sub<Output = T>,
{
    type Output = OffsetType<T>;

    fn sub(self, rhs: Self) -> Self::Output {
        OffsetType(self.0 - rhs.0)
    }
}

impl<T> Add for OffsetType<T>
where
    T: Add<Output = T>,
{
    type Output = OffsetType<T>;

    fn add(self, rhs: Self) -> Self::Output {
        OffsetType(self.0 + rhs.0)
    }
}

impl Add<i32> for o16 {
    type Output = o16;

    fn add(self, rhs: i32) -> Self::Output {
        let right_value: u16 = rhs.try_into().expect("overflow");
        OffsetType::<u16>(self.0 + right_value)
    }
}

impl Add<usize> for o16 {
    type Output = o16;

    fn add(self, rhs: usize) -> Self::Output {
        let rhs_u16: u16 = rhs.try_into().expect("rhs exceeds u16::MAX");
        OffsetType::<u16>(self.0.checked_add(rhs_u16).expect("overflow"))
    }
}

impl Mul<usize> for o16 {
    type Output = o16;

    fn mul(self, rhs: usize) -> Self::Output {
        let rhs_u16: u16 = rhs.try_into().expect("rhs exceeds u16::MAX");
        o16(rhs_u16.checked_mul(self.0).expect("overflow"))
    }
}

impl Mul<o16> for o16 {
    type Output = o16;

    fn mul(self, rhs: o16) -> Self::Output {
        o16(rhs.0 * self.0)
    }
}

impl Sub<usize> for o16 {
    type Output = o16;

    fn sub(self, rhs: usize) -> Self::Output {
        //TODO Downsizing may introduce correctness issue.
        OffsetType(self.0 - rhs as u16)
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////
pub(crate) trait ToLeBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

impl ToLeBytes for u16 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl ToLeBytes for u32 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl ToLeBytes for o16 {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
pub(crate) trait FromLeBytes {
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

impl FromLeBytes for o16 {
    fn from_bytes(bytes: Vec<u8>) -> o16 {
        OffsetType(u16::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl FromLeBytes for u32 {
    fn from_bytes(bytes: Vec<u8>) -> u32 {
        u32::from_le_bytes(bytes.try_into().unwrap())
    }
}

impl FromLeBytes for u8 {
    fn from_bytes(bytes: Vec<u8>) -> u8 {
        u8::from_le_bytes(bytes.try_into().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
#[repr(u8)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub(crate) enum PayloadType {
    Str = 1,
    U32 = 2,
    U16 = 3,
    I64 = 4,
    U8 = 5,
}

/// Payload represents a key or data payload which is persisted as pages in a database.
#[derive(Clone, Debug)]
pub(crate) struct Payload {
    buffer: Vec<u8>,
    cursor_pos: usize,
    pub payload_type: PayloadType,
}

fn stringify(data: Vec<u8>) -> String {
    String::from_utf8_lossy(data.as_slice()).to_string()
}

impl Payload {
    pub(crate) fn to_bytes(&self) -> &Vec<u8> {
        &self.buffer
    }

    pub(crate) fn to_str(&self) -> String {
        stringify(self.buffer.clone())
    }

    /// Converts a String object into a Payload instance.
    pub(crate) fn from_str(payload: String) -> Self {
        Payload {
            buffer: payload.into_bytes(),
            cursor_pos: 0,
            payload_type: PayloadType::Str,
        }
    }

    /// Converts a u32 integer into a Payload instance.
    pub(crate) fn from_u32(payload: u32) -> Self {
        Payload {
            buffer: payload.to_le_bytes().to_vec(),
            cursor_pos: 0,
            payload_type: PayloadType::U32,
        }
    }

    /// Converts a u16 integer into a Payload instance.
    pub(crate) fn from_u16(payload: u16) -> Self {
        Payload {
            buffer: payload.to_le_bytes().to_vec(),
            cursor_pos: 0,
            payload_type: PayloadType::U16,
        }
    }

    /// Converts a i64 integer into a Payload instance.
    pub(crate) fn from_i64(payload: i64) -> Self {
        Payload {
            buffer: payload.to_le_bytes().to_vec(),
            cursor_pos: 0,
            payload_type: PayloadType::I64,
        }
    }

    pub(crate) fn from_buffer(buffer: &[u8], payload_type: PayloadType) -> Self {
        Payload {
            buffer: buffer.to_vec(),
            cursor_pos: 0,
            payload_type,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.buffer.len() - self.cursor_pos
    }
}

impl Read for Payload {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = min(buf.len(), self.buffer.len() - self.cursor_pos);
        if available == 0 {
            return Ok(self.cursor_pos);
        }
        buf.copy_from_slice(&self.buffer[self.cursor_pos..self.cursor_pos + available]);
        self.cursor_pos += buf.len();
        Ok(self.cursor_pos)
    }
}

pub(crate) type Key = Payload;
