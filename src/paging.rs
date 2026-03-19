use crate::config::{get_next_page_id, update_next_page_id};
use crate::errors::InvalidPageOffsetError;
use crate::io;
use crate::io::delete_index;
use crate::types::PayloadType::Str;
use crate::types::{FromLeBytes, Key, Offset, Payload, PayloadType, ToLeBytes};
use alloc::vec::Vec;
use rand::Rng;
use serial_test::serial;
use std::cmp::min;
use std::convert::TryInto;
use std::io::Read;

const ZERO: Offset = Offset(0);
pub(crate) const PAGE_SIZE: Offset = Offset(8172);
pub(crate) const PAGE_SIZE_USIZE: usize = PAGE_SIZE.0 as usize;

// min-max ranges.
const MIN_FAN_OUT: usize = 5;
const MAX_FAN_OUT: usize = 10;
const MAX_KEY_SIZE: usize = 1024;

// Reference size constants.
const S_NUM_OF_SLOTS: usize = size_of::<Offset>();
pub(crate) const S_PAGE_ID: usize = size_of::<Offset>();
const S_PAGE_TYPE: usize = size_of::<u8>();
const S_FLAGS: usize = size_of::<u8>();
const S_LEFT_MOST: usize = size_of::<Offset>();
const S_LEFT_SIBLING: usize = size_of::<Offset>();
const S_RIGHT_SIBLING: usize = size_of::<Offset>();
const S_PARENT_PAGE_ID: usize = size_of::<Offset>();
const S_FREE_START: usize = size_of::<Offset>();
const S_FREE_END: usize = size_of::<Offset>();
const S_SLOT_TABLE_ITEM: usize = size_of::<Offset>();
const S_DATA_TYPE: usize = size_of::<u8>();
// Size of offset reference.
const S_OFFSET: usize = size_of::<Offset>();
// What we can address with an offset in a page, the data length is bound to it.
const S_DATA_LENGTH: usize = S_OFFSET;

pub const TOTAL_HEADER_SIZE: usize = S_FLAGS
    + S_RIGHT_SIBLING
    + S_LEFT_SIBLING
    + S_LEFT_MOST
    + S_PARENT_PAGE_ID
    + S_PAGE_ID
    + S_PAGE_TYPE
    + S_NUM_OF_SLOTS
    + S_FREE_START
    + S_FREE_END;

/// Slot structure as follows:
///                   ___________________________________________________________________________________
/// slot offset[0] → | payload size | payload type | key size | key type | overflow ref | key | payload |
///                   ----------------------------------------------------------------------------------
pub const SINGLE_RECORD_METADATA_SPACE_REQUIREMENT: usize =
    SINGLE_SLOT_HEADER_SIZE + S_SLOT_TABLE_ITEM;
pub const SINGLE_SLOT_HEADER_SIZE: usize = 1 * S_PAGE_ID + 2 * S_DATA_LENGTH + 2 * S_DATA_TYPE;

/// Offsets in a page header.
const OFFSET_NUM_OF_SLOTS: usize = 0;
const OFFSET_PAGE_ID: usize = OFFSET_NUM_OF_SLOTS + S_NUM_OF_SLOTS;
const OFFSET_PAGE_TYPE: usize = OFFSET_PAGE_ID + S_PAGE_ID;
const OFFSET_FLAGS: usize = OFFSET_PAGE_TYPE + S_PAGE_TYPE;
const OFFSET_LEFT_MOST: usize = OFFSET_FLAGS + S_FLAGS;
const OFFSET_LEFT_SIBLING: usize = OFFSET_LEFT_MOST + S_LEFT_MOST;
const OFFSET_RIGHT_SIBLING: usize = OFFSET_LEFT_SIBLING + S_LEFT_SIBLING;
const OFFSET_PARENT_PAGE_ID: usize = OFFSET_RIGHT_SIBLING + S_RIGHT_SIBLING;
const OFFSET_FREE_START: usize = OFFSET_PARENT_PAGE_ID + S_PARENT_PAGE_ID;
const OFFSET_FREE_END: usize = OFFSET_FREE_START + S_FREE_START;

/// Error constants
const READ_ERR: &str = "Failed to read page.";
const O_ERR: &str = "Value exceeds offset type's size.";

#[derive(Clone, Copy)]
pub struct Page {
    buffer: [u8; PAGE_SIZE_USIZE],
}

const DATA_PAGE: u8 = 0;
const INNER_PAGE: u8 = 1;

fn next_page() -> Offset {
    let mut next = get_next_page_id();
    next = next + 1;
    update_next_page_id(next);
    next
}

impl Page {
    fn new(page_type: u8) -> Self {
        Self::new_page(page_type, next_page())
    }

    fn new_page(page_type: u8, page_id: Offset) -> Self {
        let mut new_instance = Self {
            buffer: [0u8; PAGE_SIZE_USIZE],
        };

        new_instance.set_flags(0);
        new_instance.set_left_most_page_id(ZERO);
        new_instance.set_right_sibling(ZERO);
        new_instance.set_left_sibling(ZERO);
        new_instance.set_parent(ZERO);
        new_instance.set_num_of_slots(ZERO);
        new_instance.set_free_start(TOTAL_HEADER_SIZE.try_into().expect(O_ERR));
        new_instance.set_free_end(PAGE_SIZE.try_into().expect(O_ERR));
        new_instance.set_page_type(page_type);
        new_instance.set_page_id(page_id);
        new_instance
    }

    pub(crate) fn new_from(buffer: [u8; PAGE_SIZE_USIZE]) -> Self {
        Page { buffer }
    }

    pub fn new_leaf(key: Key, payload: Payload) -> Result<Offset, InvalidPageOffsetError> {
        let mut head_page = Self::new(DATA_PAGE);
        head_page.add(key, payload)
    }

    pub fn add(&mut self, key: Key, payload: Payload) -> Result<Offset, InvalidPageOffsetError> {
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

    pub fn add_left_most(&mut self, left_most_page_id: Offset) {
        self.set_left_most_page_id(left_most_page_id);
    }

    fn add_key_ref(&mut self, key: Key, payload: Payload) -> Result<(), InvalidPageOffsetError> {
        match self.add_key_data(key, payload) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn delete_key(&mut self, key: Key) -> Result<(), InvalidPageOffsetError> {
        let num_of_slots = self.num_of_slots().try_into()?;
        for i in 0..num_of_slots {
            if let Ok(current_key) = self.key_at(i)
                && key.to_str() == current_key
            {
                self.delete_slot(i);
                break
            }
        }
        Ok(())
    }

    // Adds data into a leaf node.
    // For each key, payload pair the following header metadata required:
    // | slot offset | ----> | payload size | payload type | key size | key type | overflow ref | key | payload
    fn add_key_data(
        &mut self,
        key: Key,
        mut payload: Payload,
    ) -> Result<(Payload, Offset), InvalidPageOffsetError> {
        // determine the payload and key size.
        let payload_ref = &payload;
        let key_buf = key.to_bytes();
        let key_buf_size = key_buf.len();
        let payload_size = payload.len();
        let payload_type = payload_ref.payload_type;
        let key_buf_type: PayloadType = Str;
        let slots_available = self.slots_available()?;
        if slots_available == 0 {
            panic!("No slot left!");
        }

        let available_net_free_space_for_payload = self.available_space_for_payload(key_buf_size);
        // consume the payload for available net space or payload size if it is smaller than available net space.
        let mut payload_buf = vec![0; min(available_net_free_space_for_payload?, payload_size)];
        let _ = payload.read(&mut payload_buf);
        let mut slot: Vec<u8> =
            Vec::with_capacity(Self::slot_size(key_buf_size, payload_size).try_into()?);
        let payload_size_in_offset: Offset = payload_buf.len().try_into()?;
        let key_buf_size_in_offset: Offset = key_buf_size.try_into()?;
        let overflow_page_id = if payload.len() > 0 {
            next_page()
        } else {
            Offset(0)
        };
        slot.extend_from_slice(&payload_size_in_offset.to_bytes());
        slot.extend_from_slice(&[payload_type as u8]);
        slot.extend_from_slice(&key_buf_size_in_offset.to_bytes());
        slot.extend_from_slice(&[key_buf_type as u8]);
        slot.extend_from_slice(&overflow_page_id.to_bytes());
        slot.extend_from_slice(key_buf.as_slice());
        slot.extend_from_slice(&payload_buf);

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
    fn get_overflow_data(&self) -> Result<(Vec<u8>, Offset), InvalidPageOffsetError> {
        let offset_index = TOTAL_HEADER_SIZE;
        let slot_offset = Self::read_le::<Offset, S_SLOT_TABLE_ITEM>(
            &self.buffer,
            offset_index,
            Offset::from_bytes,
        );

        let next_overflow = Self::read_le::<Offset, S_SLOT_TABLE_ITEM>(
            &self.buffer,
            slot_offset.try_into()?,
            Offset::from_bytes,
        );

        let payload_size_offset = slot_offset.get() + S_OFFSET;
        let payload_len = Self::read_le::<Offset, S_OFFSET>(
            &self.buffer,
            payload_size_offset,
            Offset::from_bytes,
        );

        let payload_offset = payload_size_offset + S_OFFSET;
        let payload: Vec<u8> = Self::read_le_into_buffer::<Vec<u8>>(
            &self.buffer,
            payload_offset,
            payload_len.try_into()?,
            |b| b,
        );

        Ok((payload, next_overflow))
    }

    /// Overflow page structure as follows:
    /// next_page_id | payload_size | payload.
    fn add_overflow_data(
        &mut self,
        mut payload: Payload,
    ) -> Result<(Payload, Offset), InvalidPageOffsetError> {
        let max_available_payload_size: usize = self
            .max_available_payload_size_in_overflow_page()
            .try_into()
            .expect(O_ERR);
        let copy_size = min(payload.len(), max_available_payload_size);
        let mut payload_in_bytes: Vec<u8> = vec![0; copy_size];
        let _ = payload.read(&mut payload_in_bytes);
        let payload_size: Offset = copy_size.try_into().expect(O_ERR);
        let mut slot: Vec<u8> = Vec::with_capacity(copy_size);
        let next_page_id = if payload.len() > 0 {
            next_page()
        } else {
            Offset(0)
        };
        slot.extend_from_slice(&next_page_id.to_bytes());
        slot.extend_from_slice(&payload_size.to_bytes());
        slot.extend_from_slice(&payload_in_bytes);
        let new_free_end = self.add_slot(&mut slot)?;
        // advance the free start and slot table with the new free end.
        self.add_to_slot_table(new_free_end)?;
        Ok((payload, next_page_id))
    }

    /// slot offset[0] → next_page_id | payload_size | payload
    fn max_available_payload_size_in_overflow_page(&self) -> Offset {
        self.free_size() - S_SLOT_TABLE_ITEM - S_DATA_LENGTH - S_PAGE_ID
    }

    fn slot_size(key_len: usize, payload_len: usize) -> Offset {
        (SINGLE_SLOT_HEADER_SIZE + key_len + payload_len)
            .try_into()
            .expect(O_ERR)
    }

    fn delete_slot(&mut self, index: usize) -> Result<(), InvalidPageOffsetError> {
        let (start, end) = self.get_slot_boundaries(index)?;
        let slot_len = end -start;
        let free_end: usize = self.free_end().try_into()?;
        if free_end < start {
            // move the entire slot block on the left by the deleted slot length, if there is a slot
            // on the left.
            let new_free_end = free_end + slot_len;
            self.buffer.copy_within(start..end, new_free_end);
            //TODO overflow handling.
            self.set_free_end(Offset(new_free_end as u16));
        }
        let buffer = vec![0u8; end - start];
        self.buffer[free_end..free_end + slot_len].copy_from_slice(&buffer);
        let num_of_slots = self.num_of_slots();
        self.set_num_of_slots(num_of_slots - 1);
        Ok(())
    }

    fn update_slot_table_item(&mut self, index: usize, offset: Offset) {
        let slot_item_offset = TOTAL_HEADER_SIZE + index * S_SLOT_TABLE_ITEM;
        let start: usize = slot_item_offset;
        let end: usize = start + S_SLOT_TABLE_ITEM;
        let new_offset_value = &offset.to_bytes();
        self.buffer[start..end].copy_from_slice(new_offset_value);
    }

    fn get_slot_boundaries(&self, index: usize) -> Result<(usize, usize), InvalidPageOffsetError> {
        let slot_offset_in_table = TOTAL_HEADER_SIZE + index * S_SLOT_TABLE_ITEM;
        let slot_offset = Self::read_le::<Offset, S_OFFSET>(
            &self.buffer,
            slot_offset_in_table,
            Offset::from_bytes,
        );

        let payload_len = Self::read_le::<Offset, S_DATA_LENGTH>(
            &self.buffer,
            slot_offset.try_into()?,
            Offset::from_bytes,
        );

        let slot_offset_usize: usize = slot_offset.try_into()?;
        let payload_type_offset = slot_offset_usize + S_DATA_LENGTH;
        let key_len_offset = payload_type_offset + S_DATA_TYPE;
        let key_len = Self::read_le::<Offset, S_DATA_LENGTH>(
            &self.buffer,
            key_len_offset,
            Offset::from_bytes,
        );

        let total_slot_size =Self::slot_size(key_len.try_into()?, payload_len.try_into()?);
        let end = slot_offset + total_slot_size;
        Ok((slot_offset_usize, end.try_into()?))
    }

    // new_free_end is the new position of the free_end after inserting a new slot at the end of the
    // page. The slots make the page grow backward:
    // | Page Header | slot table | ... free space ... | new slot | prev slot | .. |
    fn add_to_slot_table(&mut self, new_free_end: Offset) -> Result<(), InvalidPageOffsetError> {
        let free_start = self.free_start();
        let new_free_end_offset = &new_free_end.to_bytes();
        let start: usize = free_start.try_into()?;
        let end: usize = start + S_SLOT_TABLE_ITEM;
        self.buffer[start..end].copy_from_slice(new_free_end_offset);
        let size_of_slot_table_item: Offset = S_SLOT_TABLE_ITEM.try_into()?;
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
                let i_offset = i.try_into()?;
                let found = self.get_key_payload(i_offset);
                return found;
            }
        }
        Ok("".to_string())
    }

    fn get_key_payload(&self, index: Offset) -> Result<String, InvalidPageOffsetError> {
        let index_usize: usize = index.try_into()?;
        let offset_index = TOTAL_HEADER_SIZE + (index_usize * S_SLOT_TABLE_ITEM);
        let slot_offset =
            Self::read_le::<Offset, S_OFFSET>(&self.buffer, offset_index, Offset::from_bytes);
        let payload_len = Self::read_le::<Offset, S_DATA_LENGTH>(
            &self.buffer,
            slot_offset.try_into()?,
            Offset::from_bytes,
        );
        let slot_offset_usize: usize = slot_offset.try_into()?;
        let payload_type_offset = slot_offset_usize + S_DATA_LENGTH;
        let _ = Self::read_le::<u8, S_DATA_TYPE>(&self.buffer, payload_type_offset, u8::from_bytes);
        let key_len_offset = payload_type_offset + S_DATA_TYPE;
        let key_len = Self::read_le::<Offset, S_DATA_LENGTH>(
            &self.buffer,
            key_len_offset,
            Offset::from_bytes,
        );
        let key_type_offset = key_len_offset + S_DATA_LENGTH;
        let overflow_page_ref_offset = key_type_offset + S_DATA_TYPE;
        let overflow_page_ref = Self::read_le::<Offset, S_PAGE_ID>(
            &self.buffer,
            overflow_page_ref_offset,
            Offset::from_bytes,
        );
        let key_offset = overflow_page_ref_offset + S_PAGE_ID;
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

    fn add_slot(&mut self, slot: &Vec<u8>) -> Result<Offset, InvalidPageOffsetError> {
        let free_end = self.free_end();
        let new_free_end = free_end - slot.len();
        // update the buffer with key-payload.
        self.buffer[new_free_end.try_into().expect(O_ERR)..free_end.try_into()?]
            .copy_from_slice(&slot);
        self.set_free_end(new_free_end);
        debug_assert!(self.free_start() <= self.free_end());
        // As we reverse traverse the slot blocks, the old free_end becomes the start of the slot.
        Ok(new_free_end)
    }

    pub(crate) fn free_size(&self) -> Offset {
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
    fn num_of_slots(&self) -> Offset {
        Self::read_le::<Offset, S_NUM_OF_SLOTS>(
            &self.buffer,
            OFFSET_NUM_OF_SLOTS,
            Offset::from_bytes,
        )
    }

    fn set_num_of_slots(&mut self, num: Offset) {
        Self::write_le::<Offset, S_NUM_OF_SLOTS>(
            &mut self.buffer,
            OFFSET_NUM_OF_SLOTS,
            num,
            |value| value.to_bytes(),
        );
    }

    pub(crate) fn page_id(&self) -> Offset {
        Self::read_le::<Offset, S_PAGE_ID>(&self.buffer, OFFSET_PAGE_ID, |v| Offset::from_bytes(v))
    }

    fn set_page_id(&mut self, num: Offset) {
        Self::write_le::<Offset, S_PAGE_ID>(&mut self.buffer, OFFSET_PAGE_ID, num, |value| {
            value.to_bytes()
        });
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub(crate) fn page_type(&self) -> u8 {
        Self::read_le::<u8, S_PAGE_TYPE>(&self.buffer, OFFSET_PAGE_TYPE, |value| {
            u8::from_bytes(value)
        })
    }

    fn set_page_type(&mut self, num: u8) {
        Self::write_le::<u8, S_PAGE_TYPE>(&mut self.buffer, OFFSET_PAGE_TYPE, num, |value| {
            value.to_le_bytes().to_vec()
        });
    }

    fn flags(&self) -> u8 {
        Self::read_le::<u8, S_FLAGS>(&self.buffer, OFFSET_FLAGS, u8::from_bytes)
    }

    fn set_flags(&mut self, num: u8) {
        Self::write_le::<u8, S_FLAGS>(&mut self.buffer, OFFSET_FLAGS, num, |value| {
            value.to_le_bytes().to_vec()
        });
    }

    fn left_most_page_id(&self) -> Offset {
        Self::read_le::<Offset, S_LEFT_MOST>(&self.buffer, OFFSET_LEFT_MOST, Offset::from_bytes)
    }

    fn set_left_most_page_id(&mut self, num: Offset) {
        Self::write_le::<Offset, S_LEFT_MOST>(&mut self.buffer, OFFSET_LEFT_MOST, num, |value| {
            value.to_bytes()
        });
    }

    fn left_sibling(&self) -> Offset {
        Self::read_le::<Offset, S_LEFT_SIBLING>(
            &self.buffer,
            OFFSET_LEFT_SIBLING,
            Offset::from_bytes,
        )
    }

    fn set_left_sibling(&mut self, num: Offset) {
        Self::write_le::<Offset, S_LEFT_SIBLING>(
            &mut self.buffer,
            OFFSET_LEFT_SIBLING,
            num,
            |value| value.to_bytes(),
        );
    }

    fn right_sibling(&self) -> Offset {
        Self::read_le::<Offset, S_RIGHT_SIBLING>(
            &self.buffer,
            OFFSET_RIGHT_SIBLING,
            Offset::from_bytes,
        )
    }

    fn set_right_sibling(&mut self, num: Offset) {
        Self::write_le::<Offset, S_RIGHT_SIBLING>(
            &mut self.buffer,
            OFFSET_RIGHT_SIBLING,
            num,
            |value| value.to_bytes(),
        );
    }

    fn parent(&self) -> Offset {
        Self::read_le::<Offset, S_PARENT_PAGE_ID>(
            &self.buffer,
            OFFSET_PARENT_PAGE_ID,
            Offset::from_bytes,
        )
    }

    fn set_parent(&mut self, num: Offset) {
        Self::write_le::<Offset, S_PARENT_PAGE_ID>(
            &mut self.buffer,
            OFFSET_PARENT_PAGE_ID,
            num,
            |value| value.to_bytes(),
        );
    }

    pub(crate) fn free_start(&self) -> Offset {
        Self::read_le::<Offset, S_FREE_START>(&self.buffer, OFFSET_FREE_START, Offset::from_bytes)
    }

    fn set_free_start(&mut self, num: Offset) {
        Self::write_le::<Offset, S_FREE_START>(&mut self.buffer, OFFSET_FREE_START, num, |value| {
            value.to_bytes()
        });
    }

    fn free_end(&self) -> Offset {
        Self::read_le::<Offset, S_FREE_END>(&self.buffer, OFFSET_FREE_END, Offset::from_bytes)
    }

    fn set_free_end(&mut self, num: Offset) {
        Self::write_le::<Offset, S_FREE_END>(&mut self.buffer, OFFSET_FREE_END, num, |value| {
            value.to_bytes()
        });
    }

    fn key_at(&self, index: usize) -> Result<String, InvalidPageOffsetError> {
        let slot_offset = Self::read_le::<Offset, S_SLOT_TABLE_ITEM>(
            &self.buffer,
            TOTAL_HEADER_SIZE + (index * S_SLOT_TABLE_ITEM),
            Offset::from_bytes,
        );

        let slot_offset_usize: usize = slot_offset.try_into()?;
        // we don't need to read the payload length which is stored in the first register.
        // let's skip it to resolve the payload type.
        let payload_type_offset = slot_offset_usize + S_OFFSET;
        //TODO according to payload type we should use deserialization helper.
        // let payload_type = Self::read_le::<u8, SIZE_FLAGS>(&self.buffer, payload_type_offset, u8::from_bytes);
        let key_len_offset = payload_type_offset + S_PAGE_TYPE;
        let key_len = Self::read_le::<Offset, S_SLOT_TABLE_ITEM>(
            &self.buffer,
            key_len_offset,
            Offset::from_bytes,
        );

        let key_type_offset = key_len_offset + S_OFFSET;
        let overflow_page_ref_offset = key_type_offset + S_FLAGS;
        let key_offset = overflow_page_ref_offset + S_PAGE_ID;
        let key_len_usize: usize = key_len.try_into()?;
        let key_value =
            Self::read_le_into_buffer::<Vec<u8>>(&self.buffer, key_offset, key_len_usize, |b| b);

        Ok(Self::stringify(key_value))
    }
}

#[test]
#[serial]
fn test_add_slot_results_in_correct_num_of_slots() {
    let mut new_inner = Page::new_inner();
    let key1 = Payload::from_u16(123);
    let key2 = Payload::from_u16(789);
    let _ = new_inner.add_key_ref(Key::from_str("abc".to_string()), key1);
    let _ = new_inner.add_key_ref(Key::from_str("xyz".to_string()), key2);
    assert_eq!(new_inner.num_of_slots(), Offset(2));
}

#[test]
#[serial]
fn verify_available_space_empty_page() -> Result<(), InvalidPageOffsetError> {
    let new_inner = Page::new_inner();
    let available_space = new_inner.free_size();
    let total_empty_size = PAGE_SIZE - TOTAL_HEADER_SIZE;
    assert_eq!(available_space, total_empty_size);
    Ok(())
}

#[test]
#[serial]
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
        page_size - (TOTAL_HEADER_SIZE + (2 * S_SLOT_TABLE_ITEM) + (2 * slot_size));
    assert_eq!(available_space, total_empty_size);
    Ok(())
}

#[test]
#[serial]
fn verify_read_the_inserted() {
    let mut new_inner = Page::new_inner();
    let payload1 = Payload::from_str("123".to_string());
    let payload2 = Payload::from_str("234".to_string());
    let _ = new_inner.add_key_ref(Key::from_str("abcdefh".to_string()), payload1);
    let _ = new_inner.add_key_ref(Key::from_str("xyz".to_string()), payload2);
    match new_inner.get_key_payload(ZERO) {
        Ok(payload) => {
            assert_eq!(payload, "123");
        }
        Err(_) => assert!(false),
    }

    match new_inner.get_key_payload(Offset(1)) {
        Ok(payload) => {
            assert_eq!(payload, "234");
        }
        Err(_) => assert!(false),
    }
}

#[test]
#[serial]
fn verify_add_data_node_less_than_page_size() -> Result<(), InvalidPageOffsetError> {
    let page_size: usize = PAGE_SIZE.try_into()?;
    let string = random_string(100);
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
#[serial]
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
    let available_space = available_bytes.try_into()?;
    let payload_string = random_string(available_space);
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
#[serial]
fn verify_add_second_payload_larger_than_available_size() -> Result<(), InvalidPageOffsetError> {
    delete_index();
    let page_size: usize = PAGE_SIZE.try_into()?;
    // one head page and two overflow pages expected.
    let input_value = random_string(page_size * 2);
    assert!(input_value.len() > page_size);
    let data_node = Page::new_leaf(
        Key::from_str("foo".to_string()),
        Payload::from_str(input_value.clone()),
    )?;
    let page_id: usize = data_node.try_into()?;
    let second_input = random_string(page_size * 2);
    add_to_page(page_id, "bar".to_string(), second_input.clone());
    let leading_page = io::read(page_id).expect(READ_ERR);
    {
        let guard = leading_page.lock().unwrap();
        let num_of_slots: usize = guard.num_of_slots().try_into()?;
        assert_eq!(num_of_slots, 2);
        let bar_value = guard.get_for_key(Key::from_str("bar".to_string()));
        if let Ok(a) = bar_value {
            assert_eq!(second_input.clone(), a);
        }
    }
    Ok(())
}

fn add_to_page(page_id: usize, key: String, second_input: String) {
    let leading_page = io::read(page_id).expect(READ_ERR);
    {
        let mut mutex = leading_page.lock().unwrap();
        let _ = mutex
            .add(Key::from_str(key), Payload::from_str(second_input))
            .unwrap();
    };
}

#[test]
#[serial]
fn verify_add_payload_larger_than_available_size() -> Result<(), InvalidPageOffsetError> {
    delete_index();
    let page_size: usize = PAGE_SIZE.try_into()?;
    // one head page and two overflow pages expected.
    let input_value = random_string(page_size * 2);
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

#[test]
#[serial]
#[should_panic(expected = "No slot left!")]
fn verify_max_fan_out() {
    delete_index();
    let input_value = "".to_string();
    let data_node_id = Page::new_leaf(
        Key::from_str("foo".to_string()),
        Payload::from_str(input_value.clone()),
    )
    .unwrap()
    .get();

    // Fill the page slots up with overflowing payloads.
    for i in 0..MAX_FAN_OUT + 1 {
        let random_key = random_string(3);
        add_to_page(data_node_id, random_key, input_value.clone());
    }

    let page = io::read(data_node_id).expect(READ_ERR);
    {
        let mutex = page.lock().unwrap();
        let free_size: usize = mutex.free_size().try_into().expect(O_ERR);
        assert_eq!(free_size, 0)
    }
}

#[test]
#[serial]
fn verify_next_page_id() {
    delete_index();
    assert_eq!(next_page(), Offset(1));
    assert_eq!(next_page(), Offset(2));
}

// This test ensures minimum fan-out in case all payloads exceeds the page capacity.
#[test]
#[serial]
fn verify_no_space_left_in_head_after_inserting_overflowed_pages() {
    delete_index();
    let page_size: usize = PAGE_SIZE.try_into().expect(O_ERR);
    let input_value = random_string(page_size * 2);
    assert!(input_value.len() > page_size);
    let data_node_id = Page::new_leaf(
        Key::from_str("foo".to_string()),
        Payload::from_str(input_value.clone()),
    )
    .unwrap()
    .get();

    // Fill the page slots up with overflowing payloads.
    for i in 0..MIN_FAN_OUT - 1 {
        let random_key = random_string(9);
        add_to_page(data_node_id, random_key, input_value.clone());
    }

    let page = io::read(data_node_id).expect(READ_ERR);
    {
        let mutex = page.lock().unwrap();
        let free_size: usize = mutex.free_size().try_into().expect(O_ERR);
        assert_eq!(free_size, 0)
    }
}

fn random_string(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let s: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    s
}
