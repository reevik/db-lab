use crate::paging::S_PAGE_ID;
use crate::types::{FromLeBytes, Offset, ToLeBytes};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

const CONFIG_FILE: &str = "config";
const O_NEXT_PAGE_ID: u64 = 0;
const TOTAL_CONFIG_SIZE: u64 = O_NEXT_PAGE_ID + size_of::<u64>() as u64;

pub(crate) fn get_next_page_id() -> Offset {
    let mut buffer = [0u8; S_PAGE_ID];
    let page_id = read_from_disk(O_NEXT_PAGE_ID, &mut buffer);
    Offset::from_bytes(page_id.to_vec().try_into().unwrap())
}

pub(crate) fn update_next_page_id(next_page_id: Offset) {
    write_to_disk(O_NEXT_PAGE_ID, &next_page_id.to_bytes())
}

fn write_to_disk(offset: u64, data: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(CONFIG_FILE)
        .unwrap();
    let _ = file.seek(SeekFrom::Start(offset));
    let _ = file.write_all(data);
    let _ = file.sync_all();
}

fn read_from_disk(offset: u64, buffer: &mut [u8]) -> &[u8] {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(CONFIG_FILE)
        .unwrap();

    let file_size = file.metadata().unwrap().len();
    if file_size == 0 {
        println!("Config file size mismatch. Setting defaults.");
        write_to_disk(O_NEXT_PAGE_ID, Offset(0).to_bytes().as_slice());
    }

    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read(buffer).unwrap();
    buffer
}
