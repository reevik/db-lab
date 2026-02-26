use crate::errors::InvalidPageOffsetError;
use crate::io;
use crate::io::delete_index;
use crate::types::PayloadType::Str;
use crate::types::{o16, FromLeBytes, Key, Payload, PayloadType, ToLeBytes};
use alloc::vec::Vec;
use rand::Rng;
use std::cmp::min;
use std::convert::TryInto;
use std::io::Read;

const ZERO: o16 = o16(0);
static mut NEXT_PAGE_ID: o16 = o16(0);
pub(crate) const PAGE_SIZE: o16 = o16(8172);
pub(crate) const PAGE_SIZE_USIZE: usize = PAGE_SIZE.0 as usize;
const MIN_FAN_OUT: usize = 5;
const MAX_FAN_OUT: usize = 5;
const MAX_KEY_SIZE: usize = 1024;
const SIZE_NUM_OF_SLOTS: usize = size_of::<o16>();
const SIZE_PAGE_ID: usize = size_of::<o16>();
const SIZE_PAGE_TYPE: usize = size_of::<u8>();
const SIZE_FLAGS: usize = size_of::<u8>();
const SIZE_LEFT_MOST: usize = size_of::<o16>();
const SIZE_LEFT_SIBLING: usize = size_of::<o16>();
const SIZE_RIGHT_SIBLING: usize = size_of::<o16>();
const SIZE_PARENT_PAGE_ID: usize = size_of::<o16>();
const SIZE_FREE_START: usize = size_of::<o16>();
const SIZE_FREE_END: usize = size_of::<o16>();
const SIZE_OF_SLOT_TABLE_ITEM: usize = size_of::<o16>();
const SIZE_OFFSET: usize = size_of::<o16>();

pub const TOTAL_HEADER_SIZE: usize = SIZE_FLAGS
    + SIZE_RIGHT_SIBLING
    + SIZE_LEFT_SIBLING
    + SIZE_LEFT_MOST
    + SIZE_PARENT_PAGE_ID
    + SIZE_PAGE_ID
    + SIZE_PAGE_TYPE
    + SIZE_NUM_OF_SLOTS
    + SIZE_FREE_START
    + SIZE_FREE_END;

pub const SINGLE_RECORD_METADATA_SPACE_REQUIREMENT: usize =
    SINGLE_SLOT_HEADER_SIZE + SIZE_OF_SLOT_TABLE_ITEM;

// 1x key size
// 1x payload size
// 1x overflow page ref.
// key type
// payload type.
pub const SINGLE_SLOT_HEADER_SIZE: usize = 3 * SIZE_PAGE_ID + 2 * SIZE_PAGE_TYPE;

/// Offsets in the
const OFFSET_NUM_OF_SLOTS: usize = 0;
const OFFSET_PAGE_ID: usize = OFFSET_NUM_OF_SLOTS + SIZE_NUM_OF_SLOTS;
const OFFSET_PAGE_TYPE: usize = OFFSET_PAGE_ID + SIZE_PAGE_ID;
const OFFSET_FLAGS: usize = OFFSET_PAGE_TYPE + SIZE_PAGE_TYPE;
const OFFSET_LEFT_MOST: usize = OFFSET_FLAGS + SIZE_FLAGS;
const OFFSET_LEFT_SIBLING: usize = OFFSET_LEFT_MOST + SIZE_LEFT_MOST;
const OFFSET_RIGHT_SIBLING: usize = OFFSET_LEFT_SIBLING + SIZE_LEFT_SIBLING;
const OFFSET_PARENT_PAGE_ID: usize = OFFSET_RIGHT_SIBLING + SIZE_RIGHT_SIBLING;
const OFFSET_FREE_START: usize = OFFSET_PARENT_PAGE_ID + SIZE_PARENT_PAGE_ID;
const OFFSET_FREE_END: usize = OFFSET_FREE_START + SIZE_FREE_START;

#[derive(Clone, Copy)]
pub struct Page {
    buffer: [u8; PAGE_SIZE_USIZE],
}

const DATA_PAGE: u8 = 0;
const INNER_PAGE: u8 = 1;

fn next_page() -> o16 {
    unsafe {
        let next = NEXT_PAGE_ID;
        NEXT_PAGE_ID = o16(NEXT_PAGE_ID.0 + 1);
        next
    }
}

impl Page {
    fn new(page_type: u8) -> Self {
        Self::new_page(page_type, next_page())
    }

    fn new_page(page_type: u8, page_id: o16) -> Self {
        let mut new_instance = Self {
            buffer: [0u8; PAGE_SIZE_USIZE],
        };

        new_instance.set_flags(0);
        new_instance.set_left_most_page_id(ZERO);
        new_instance.set_right_sibling(ZERO);
        new_instance.set_left_sibling(ZERO);
        new_instance.set_parent(ZERO);
        new_instance.set_num_of_slots(ZERO);
        new_instance.set_free_start(TOTAL_HEADER_SIZE.try_into().expect("Too many pages"));
        new_instance.set_free_end(PAGE_SIZE.try_into().expect(""));
        new_instance.set_page_type(page_type);
        new_instance.set_page_id(page_id);
        new_instance
    }

    pub(crate) fn new_from(buffer: [u8; PAGE_SIZE_USIZE]) -> Self {
        Page { buffer }
    }

    pub fn new_leaf(key: Key, payload: Payload) -> Result<o16, InvalidPageOffsetError> {
        let mut head_page = Self::new(DATA_PAGE);
        head_page.add(key, payload)
    }

    pub fn add(&mut self, key: Key, payload: Payload) -> Result<o16, InvalidPageOffsetError> {
        let head_page = self;
        let current_page_id = head_page.page_id();
        let mut current_page = head_page;
        let payload_and_page_id = current_page.add_key_data(key, payload)?;
        let mut residual = payload_and_page_id.0;
        let mut page_id = payload_and_page_id.1;
        io::write(&current_page);
        while residual.len() > 0 {
            let mut current_page = Self::new_page(DATA_PAGE, page_id);
            let overflow = current_page.add_overflow_data(residual)?;
            io::write(&current_page);
            residual = overflow.0;
            page_id = overflow.1;
        }
        Ok(current_page_id)
    }

    pub fn new_inner() -> Self {
        Self::new(INNER_PAGE)
    }

    pub fn add_left_most(&mut self, left_most_page_id: o16) {
        self.set_left_most_page_id(left_most_page_id);
    }

    fn add_key_ref(&mut self, key: Key, payload: Payload) -> Result<(), InvalidPageOffsetError> {
        match self.add_key_data(key, payload) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // Adds data into a leaf node.
    fn add_key_data(
        &mut self,
        key: Key,
        mut payload: Payload,
    ) -> Result<(Payload, o16), InvalidPageOffsetError> {
        // determine the payload and key size.
        let payload_ref = &payload;
        let key_buf = key.to_bytes();
        let key_buf_size = key_buf.len();
        let payload_size = payload.len();
        let payload_type = payload_ref.payload_type;
        let key_buf_type: PayloadType = Str;

        // For each key, payload pair the following header metadata required:
        // | slot offset | ----> | payload size | payload type | key size | key type | overflow ref | key | payload
        let slots_available = self.slots_available()?;
        if slots_available == 0 {
            panic!("No slot left!");
        }

        let available_net_free_space_for_payload = self.available_space_for_payload(key_buf_size);
        // consume the payload for available net space or payload size if it is smaller than available net space.
        let mut read_buf = vec![0; min(available_net_free_space_for_payload?, payload_size)];
        let _ = payload.read(&mut read_buf);
        let mut slot: Vec<u8> =
            Vec::with_capacity(Self::slot_size(key_buf_size, payload_size).try_into()?);
        let payload_size_in_o16: o16 = read_buf.len().try_into()?;
        let key_buf_size_in_o16: o16 = key_buf_size.try_into()?;
        let overflow_page_id = if payload.len() > 0 {
            next_page()
        } else {
            o16(0)
        };
        slot.extend_from_slice(&payload_size_in_o16.to_bytes());
        slot.extend_from_slice(&[payload_type as u8]);
        slot.extend_from_slice(&key_buf_size_in_o16.to_bytes());
        slot.extend_from_slice(&[key_buf_type as u8]);
        slot.extend_from_slice(&overflow_page_id.to_bytes());
        slot.extend_from_slice(key_buf.as_slice());
        slot.extend_from_slice(&read_buf);

        let new_free_end = self.add_slot(&mut slot)?;
        // advance the free start and slot table with the new free end.
        self.add_to_slot_table(new_free_end)?;
        Ok((payload, overflow_page_id))
    }

    // reserve minimum required space for residual slots.
    fn available_space_for_payload(
        &mut self,
        key_buf_size: usize,
    ) -> Result<usize, InvalidPageOffsetError> {
        let slots_available = self.slots_available()?;
        if slots_available == 0 {
            return Ok(0);
        }
        let free_space: usize = self.free_size().try_into()?;
        let single_record_reservation = SINGLE_RECORD_METADATA_SPACE_REQUIREMENT + MAX_KEY_SIZE;

        Ok(
            free_space
            - SINGLE_RECORD_METADATA_SPACE_REQUIREMENT // headroom for the current key-payload.
            - key_buf_size // current key.
            - ((slots_available - 1) * single_record_reservation), // reserved headroom to satisfy min. requirements.
        )
    }

    fn slots_available(&mut self) -> Result<usize, InvalidPageOffsetError> {
        let num_of_slots: usize = self.num_of_slots().try_into()?;
        let slots_available: usize = if num_of_slots == MAX_FAN_OUT {
            0
        } else {
            MIN_FAN_OUT - num_of_slots
        };
        Ok(slots_available)
    }

    // Read offset payload as a vector of bytes.
    fn get_overflow_data(&self) -> Result<(Vec<u8>, o16), InvalidPageOffsetError> {
        let offset_index = TOTAL_HEADER_SIZE;
        let slot_offset = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            offset_index,
            o16::from_bytes,
        );

        let next_overflow = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            slot_offset.try_into()?,
            o16::from_bytes,
        );

        let slot_offset_usize: usize = slot_offset.try_into()?;
        let payload_size_offset = slot_offset_usize + SIZE_PAGE_ID;
        let payload_len =
            Self::read_le::<o16, SIZE_PAGE_ID>(&self.buffer, payload_size_offset, o16::from_bytes);

        let payload_offset = payload_size_offset + SIZE_PAGE_ID;
        let payload: Vec<u8> = Self::read_le_into_buffer::<Vec<u8>>(
            &self.buffer,
            payload_offset,
            payload_len.try_into()?,
            |b| b,
        );

        Ok((payload, next_overflow))
    }
    fn add_overflow_data(
        &mut self,
        mut payload: Payload,
    ) -> Result<(Payload, o16), InvalidPageOffsetError> {
        let max_available_payload_size: usize = self
            .max_available_payload_size_in_overflow_page()
            .try_into()
            .expect("Too many pages");
        let copy_size = min(payload.len(), max_available_payload_size);
        let mut payload_in_bytes: Vec<u8> = vec![0; copy_size];
        let _ = payload.read(&mut payload_in_bytes);
        let copy_size_o16: o16 = copy_size.try_into().expect("Too many pages");
        let mut slot: Vec<u8> = Vec::with_capacity(copy_size);
        let next_page_id = if payload.len() > 0 {
            next_page()
        } else {
            o16(0)
        };
        slot.extend_from_slice(&next_page_id.to_bytes());
        slot.extend_from_slice(&copy_size_o16.to_bytes());
        slot.extend_from_slice(&payload_in_bytes);
        let new_free_end = self.add_slot(&mut slot)?;
        // advance the free start and slot table with the new free end.
        self.add_to_slot_table(new_free_end)?;
        Ok((payload, next_page_id))
    }

    fn max_available_payload_size_in_overflow_page(&self) -> o16 {
        self.free_size() - 3 * SIZE_OF_SLOT_TABLE_ITEM
    }

    fn slot_size(key_len: usize, payload_len: usize) -> o16 {
        (SINGLE_SLOT_HEADER_SIZE + key_len + payload_len)
            .try_into()
            .expect("Too many pages")
    }

    // new_free_end is the new position of the free_end after inserting a new slot at the end of the
    // page. The slots make the page grow backward:
    // | Page Header | slot table | ... free space ... | new slot | prev slot | .. |
    fn add_to_slot_table(&mut self, new_free_end: o16) -> Result<(), InvalidPageOffsetError> {
        let free_start = self.free_start();
        let new_free_end_offset = &new_free_end.to_bytes();
        let start: usize = free_start.try_into()?;
        let end: usize = start + SIZE_OF_SLOT_TABLE_ITEM;
        self.buffer[start..end].copy_from_slice(new_free_end_offset);
        let size_of_slot_table_item: o16 = SIZE_OF_SLOT_TABLE_ITEM.try_into()?;
        self.set_free_start(free_start + size_of_slot_table_item);
        self.set_num_of_slots(self.num_of_slots() + 1);
        debug_assert!(self.free_start() <= self.free_end());
        Ok(())
    }

    fn get_for_key(&self, key: Key) -> Result<String, InvalidPageOffsetError> {
        let num_of_slots = self.num_of_slots().try_into()?;
        for i in 0..num_of_slots {
            if let Ok(current_key) = self.key_at(i)
                && key.to_str() == current_key
            {
                let i_o16 = i.try_into()?;
                let found = self.get_key_payload(i_o16);
                return found;
            }
        }
        Ok("".to_string())
    }

    fn get_key_payload(&self, index: o16) -> Result<String, InvalidPageOffsetError> {
        let index_usize: usize = index.try_into()?;
        let o16_size: usize = size_of::<o16>();
        let offset_index = TOTAL_HEADER_SIZE + (index_usize * o16_size);
        let slot_offset = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            offset_index,
            o16::from_bytes,
        );
        let payload_len = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            slot_offset.try_into()?,
            o16::from_bytes,
        );
        let slot_offset_usize: usize = slot_offset.try_into()?;
        let page_type_size: usize = SIZE_PAGE_ID;
        let payload_type_offset = slot_offset_usize + page_type_size;
        let payload_type =
            Self::read_le::<u8, SIZE_FLAGS>(&self.buffer, payload_type_offset, u8::from_bytes);
        let key_len_offset = payload_type_offset + SIZE_FLAGS;
        let key_len = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            key_len_offset,
            o16::from_bytes,
        );
        let key_type_offset = key_len_offset + SIZE_PAGE_ID;
        let overflow_page_ref_offset = key_type_offset + SIZE_FLAGS;
        let overflow_page_ref = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            overflow_page_ref_offset,
            o16::from_bytes,
        );
        let key_offset = overflow_page_ref_offset + SIZE_PAGE_ID;
        let key_len_usize: usize = key_len.try_into()?;
        let page_size: usize = PAGE_SIZE.try_into()?;
        let max_payload_capacity = page_size
            - (key_len_usize + TOTAL_HEADER_SIZE + SINGLE_RECORD_METADATA_SPACE_REQUIREMENT);
        let payload_offset = key_offset + key_len_usize;
        let mut payload = Self::read_le_into_buffer::<Vec<u8>>(
            &self.buffer,
            payload_offset,
            min(max_payload_capacity, payload_len.try_into()?),
            |b| b,
        );
        let mut current_right_sibling = overflow_page_ref;
        if current_right_sibling == ZERO {
            return Ok(Self::stringify(payload));
        }

        loop {
            if current_right_sibling == ZERO {
                break;
            }

            let current_right_sibling_id: usize = current_right_sibling.try_into()?;
            current_right_sibling = match io::read(current_right_sibling_id) {
                Some(overflow_page) => {
                    let mutex = overflow_page.lock().unwrap();
                    if let Ok((overflow_data, next_overflow)) = mutex.get_overflow_data() {
                        payload.extend_from_slice(&overflow_data);
                        next_overflow
                    } else {
                        ZERO
                    }
                }
                None => {
                    panic!("Page ID out of bounds");
                }
            };
        }

        Ok(Self::stringify(payload))
    }

    fn stringify(data: Vec<u8>) -> String {
        String::from_utf8_lossy(data.as_slice()).to_string()
    }

    fn add_slot(&mut self, slot: &Vec<u8>) -> Result<o16, InvalidPageOffsetError> {
        let free_end = self.free_end();
        let new_free_end = free_end - slot.len();
        // update the buffer with key-payload.
        self.buffer[new_free_end.try_into().expect("")..free_end.try_into()?]
            .copy_from_slice(&slot);
        self.set_free_end(new_free_end);
        debug_assert!(self.free_start() <= self.free_end());
        // As we reverse traverse the slot blocks, the old free_end becomes the start of the slot.
        Ok(new_free_end)
    }

    pub(crate) fn free_size(&self) -> o16 {
        self.free_end() - self.free_start()
    }

    fn read_le<T, const N: usize>(buf: &[u8], offset: usize, f: fn(Vec<u8>) -> T) -> T {
        let slice = &buf[offset..offset + N];
        let arr: [u8; N] = slice.try_into().expect("slice length mismatch");
        f(arr.to_vec())
    }

    fn read_le_into_buffer<T>(buf: &[u8], offset: usize, length: usize, f: fn(Vec<u8>) -> T) -> T {
        let buffer_ref = buf[offset..offset + length].to_vec();
        f(buffer_ref)
    }

    fn write_le<T, const N: usize>(buf: &mut [u8], offset: usize, value: T, f: fn(T) -> Vec<u8>) {
        let bytes = f(value);
        buf[offset..offset + N].copy_from_slice(&bytes);
    }

    /// Returns the number of slots from the first two bytes in the page.
    fn num_of_slots(&self) -> o16 {
        Self::read_le::<o16, SIZE_NUM_OF_SLOTS>(&self.buffer, OFFSET_NUM_OF_SLOTS, o16::from_bytes)
    }

    fn set_num_of_slots(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_NUM_OF_SLOTS>(
            &mut self.buffer,
            OFFSET_NUM_OF_SLOTS,
            num,
            |value| value.to_bytes(),
        );
    }

    pub(crate) fn page_id(&self) -> o16 {
        Self::read_le::<o16, SIZE_PAGE_ID>(&self.buffer, OFFSET_PAGE_ID, |v| o16::from_bytes(v))
    }

    fn set_page_id(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_PAGE_ID>(&mut self.buffer, OFFSET_PAGE_ID, num, |value| {
            value.to_bytes()
        });
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub(crate) fn page_type(&self) -> u8 {
        Self::read_le::<u8, SIZE_PAGE_TYPE>(&self.buffer, OFFSET_PAGE_TYPE, |value| {
            u8::from_bytes(value)
        })
    }

    fn set_page_type(&mut self, num: u8) {
        Self::write_le::<u8, SIZE_PAGE_TYPE>(&mut self.buffer, OFFSET_PAGE_TYPE, num, |value| {
            value.to_le_bytes().to_vec()
        });
    }

    fn flags(&self) -> u8 {
        Self::read_le::<u8, SIZE_FLAGS>(&self.buffer, OFFSET_FLAGS, u8::from_bytes)
    }

    fn set_flags(&mut self, num: u8) {
        Self::write_le::<u8, SIZE_FLAGS>(&mut self.buffer, OFFSET_FLAGS, num, |value| {
            value.to_le_bytes().to_vec()
        });
    }

    fn left_most_page_id(&self) -> o16 {
        Self::read_le::<o16, SIZE_LEFT_MOST>(&self.buffer, OFFSET_LEFT_MOST, o16::from_bytes)
    }

    fn set_left_most_page_id(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_LEFT_MOST>(&mut self.buffer, OFFSET_LEFT_MOST, num, |value| {
            value.to_bytes()
        });
    }

    fn left_sibling(&self) -> o16 {
        Self::read_le::<o16, SIZE_LEFT_SIBLING>(&self.buffer, OFFSET_LEFT_SIBLING, o16::from_bytes)
    }

    fn set_left_sibling(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_LEFT_SIBLING>(
            &mut self.buffer,
            OFFSET_LEFT_SIBLING,
            num,
            |value| value.to_bytes(),
        );
    }

    fn right_sibling(&self) -> o16 {
        Self::read_le::<o16, SIZE_RIGHT_SIBLING>(
            &self.buffer,
            OFFSET_RIGHT_SIBLING,
            o16::from_bytes,
        )
    }

    fn set_right_sibling(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_RIGHT_SIBLING>(
            &mut self.buffer,
            OFFSET_RIGHT_SIBLING,
            num,
            |value| value.to_bytes(),
        );
    }

    fn parent(&self) -> o16 {
        Self::read_le::<o16, SIZE_PARENT_PAGE_ID>(
            &self.buffer,
            OFFSET_PARENT_PAGE_ID,
            o16::from_bytes,
        )
    }

    fn set_parent(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_PARENT_PAGE_ID>(
            &mut self.buffer,
            OFFSET_PARENT_PAGE_ID,
            num,
            |value| value.to_bytes(),
        );
    }

    pub(crate) fn free_start(&self) -> o16 {
        Self::read_le::<o16, SIZE_FREE_START>(&self.buffer, OFFSET_FREE_START, o16::from_bytes)
    }

    fn set_free_start(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_FREE_START>(&mut self.buffer, OFFSET_FREE_START, num, |value| {
            value.to_bytes()
        });
    }

    fn free_end(&self) -> o16 {
        Self::read_le::<o16, SIZE_FREE_END>(&self.buffer, OFFSET_FREE_END, o16::from_bytes)
    }

    fn set_free_end(&mut self, num: o16) {
        Self::write_le::<o16, SIZE_FREE_END>(&mut self.buffer, OFFSET_FREE_END, num, |value| {
            value.to_bytes()
        });
    }

    fn key_at(&self, index: usize) -> Result<String, InvalidPageOffsetError> {
        let slot_offset = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            TOTAL_HEADER_SIZE + (index * SIZE_PAGE_ID),
            o16::from_bytes,
        );

        let slot_offset_usize: usize = slot_offset.try_into()?;
        let page_type_size: usize = SIZE_PAGE_ID;
        let payload_type_offset = slot_offset_usize + page_type_size;
        //TODO according to payload type we should use deserialization helper.
        // let payload_type = Self::read_le::<u8, SIZE_FLAGS>(&self.buffer, payload_type_offset, u8::from_bytes);
        let key_len_offset = payload_type_offset + SIZE_FLAGS;
        let key_len = Self::read_le::<o16, SIZE_OF_SLOT_TABLE_ITEM>(
            &self.buffer,
            key_len_offset,
            o16::from_bytes,
        );

        let key_type_offset = key_len_offset + SIZE_PAGE_ID;
        let overflow_page_ref_offset = key_type_offset + SIZE_FLAGS;
        let key_offset = overflow_page_ref_offset + SIZE_PAGE_ID;
        let key_len_usize: usize = key_len.try_into()?;
        let key_value =
            Self::read_le_into_buffer::<Vec<u8>>(&self.buffer, key_offset, key_len_usize, |b| b);

        Ok(Self::stringify(key_value))
    }
}

#[test]
fn test_add_slot_results_in_correct_num_of_slots() {
    let mut new_inner = Page::new_inner();
    let key1 = Payload::from_u16(123);
    let key2 = Payload::from_u16(789);
    let _ = new_inner.add_key_ref(Key::from_str("abc".to_string()), key1);
    let _ = new_inner.add_key_ref(Key::from_str("xyz".to_string()), key2);
    assert_eq!(new_inner.num_of_slots(), o16(2));
}

#[test]
fn verify_available_space_empty_page() -> Result<(), InvalidPageOffsetError> {
    let new_inner = Page::new_inner();
    let available_space = new_inner.free_size();
    let total_empty_size = PAGE_SIZE - TOTAL_HEADER_SIZE;
    assert_eq!(available_space, total_empty_size);
    Ok(())
}

#[test]
fn verify_available_space_after_insertion() -> Result<(), InvalidPageOffsetError> {
    let key1 = Key::from_str("foo".to_string());
    let key2 = Key::from_str("foo".to_string());
    let payload = Payload::from_str("123".to_string());
    let payload_len = payload.len();
    let mut new_inner = Page::new_inner();
    let _ = new_inner.add_key_ref(key1.clone(), payload.clone());
    let _ = new_inner.add_key_ref(key2, payload);
    let available_space: usize = new_inner.free_size().try_into()?;
    let slot_size: usize = Page::slot_size(key1.len(), payload_len).try_into()?;
    let page_size: usize = PAGE_SIZE.try_into()?;
    let total_empty_size: usize =
        page_size - (TOTAL_HEADER_SIZE + (2 * SIZE_OF_SLOT_TABLE_ITEM) + (2 * slot_size));
    assert_eq!(available_space, total_empty_size);
    Ok(())
}

#[test]
fn verify_read_the_inserted() {
    let mut new_inner = Page::new_inner();
    let payload1 = Payload::from_str("123".to_string());
    let payload2 = Payload::from_str("234".to_string());
    let _ = new_inner.add_key_ref(Key::from_str("abcdefg".to_string()), payload1);
    let _ = new_inner.add_key_ref(Key::from_str("xyz".to_string()), payload2);
    match new_inner.get_key_payload(ZERO) {
        Ok(payload) => {
            assert_eq!(payload, "123");
        }
        Err(_) => assert!(false),
    }

    match new_inner.get_key_payload(o16(1)) {
        Ok(payload) => {
            assert_eq!(payload, "234");
        }
        Err(_) => assert!(false),
    }
}

#[test]
fn verify_add_data_node_less_than_page_size() -> Result<(), InvalidPageOffsetError> {
    let page_size: usize = PAGE_SIZE.try_into()?;
    let string = random_string(o16::from_u16(100u16));
    assert!(string.len() < page_size);
    let data_node = Page::new_leaf(Key::from_str("foo".to_string()), Payload::from_str(string))?;
    let page = io::read(data_node.0 as usize);
    if let Some(leading_page) = page {
        let mutex = leading_page.lock().unwrap();
        assert!(mutex.free_end() > mutex.free_start());
    } else {
        assert!(false);
    }

    Ok(())
}

#[test]
fn verify_add_data_node_full_page() -> Result<(), InvalidPageOffsetError> {
    let key = Key::from_str("foo".to_string());
    let max_page_size: usize = PAGE_SIZE.try_into()?;
    // available bytes consists of available space excluding the page header, one slot header
    // requirements, and the rest reserved for remaining slots, and key length.
    let available_bytes = PAGE_SIZE
        - (TOTAL_HEADER_SIZE
            + SINGLE_RECORD_METADATA_SPACE_REQUIREMENT
            + key.len()
            + ((MIN_FAN_OUT - 1) * (SINGLE_RECORD_METADATA_SPACE_REQUIREMENT + MAX_KEY_SIZE)));
    let payload_string = random_string(available_bytes);
    assert!(payload_string.len() < max_page_size);
    let data_node = Page::new_leaf(key, Payload::from_str(payload_string))?;
    let page = io::read(data_node.0 as usize);
    if let Some(leading_page) = page {
        let mutex = leading_page.lock().unwrap();
        let free_space: usize = mutex.free_size().try_into()?;
        assert_eq!(
            free_space,
            (MIN_FAN_OUT - 1) * (SINGLE_RECORD_METADATA_SPACE_REQUIREMENT + MAX_KEY_SIZE)
        );
    } else {
        assert!(false);
    }
    Ok(())
}

#[test]
fn verify_add_second_payload_larger_than_available_size() -> Result<(), InvalidPageOffsetError> {
    delete_index();
    let page_size: usize = PAGE_SIZE.try_into()?;
    // one head page and two overflow pages expected.
    let input_value = random_string(PAGE_SIZE * 2);
    assert!(input_value.len() > page_size);
    let data_node = Page::new_leaf(
        Key::from_str("foo".to_string()),
        Payload::from_str(input_value.clone()),
    )?;
    let page_id: usize = data_node.try_into()?;
    let page = io::read(page_id);
    let second_input = random_string(PAGE_SIZE * 2);
    if let Some(leading_page) = page {
        // first read data from disk and add another payload into the page.
        let mut mutex = leading_page.lock().unwrap();
        let _ = mutex.add(
            Key::from_str("bar".to_string()),
            Payload::from_str(second_input.clone()),
        )?;
    }
    let page = io::read(page_id);
    if let Some(leading_page) = page {
        let mutex = leading_page.lock().unwrap();
        let num_of_slots: usize = mutex.num_of_slots().try_into()?;
        assert_eq!(num_of_slots, 2);
        let bar_value = mutex.get_for_key(Key::from_str("bar".to_string()));
        if let Ok(a) = bar_value {
            assert_eq!(second_input.clone(), a);
        }
    }
    Ok(())
}

#[test]
fn verify_add_payload_larger_than_available_size() -> Result<(), InvalidPageOffsetError> {
    delete_index();
    let page_size: usize = PAGE_SIZE.try_into()?;
    // one head page and two overflow pages expected.
    let input_value = random_string(PAGE_SIZE * 2);
    assert!(input_value.len() > page_size);
    let data_node = Page::new_leaf(
        Key::from_str("foo".to_string()),
        Payload::from_str(input_value.clone()),
    )?;

    let page = io::read(data_node.0 as usize);
    // we read the first item in the list.
    let record_index = ZERO;
    if let Some(leading_page) = page {
        let mutex = leading_page.lock().unwrap();
        if let Ok(payload) = mutex.get_key_payload(record_index) {
            assert_eq!(input_value, payload)
        }
    } else {
        assert!(false);
    }

    Ok(())
}

fn random_string(len: o16) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let s: String = (0..len.get())
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    s
}
