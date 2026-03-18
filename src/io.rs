use crate::paging::{Page, PAGE_SIZE, PAGE_SIZE_USIZE};
use crate::types::Offset;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

// in-memory cache which holds page ids to Page objects.
static CACHE: Lazy<Mutex<HashMap<Offset, Arc<Mutex<Page>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

const INDEX_FILE: &str = "index.000";

pub(crate) fn write(page: &Page) {
    let page_id: usize = page.page_id().try_into().unwrap();
    let page_size: usize = PAGE_SIZE.try_into().unwrap();
    let file_offset: usize = page_id * page_size;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(INDEX_FILE)
        .unwrap();
    let _ = file.seek(SeekFrom::Start(file_offset.try_into().unwrap()));
    let _ = file.write_all(page.buffer());
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    cache.insert(page.page_id(), Arc::new(Mutex::new(page.clone())));
}

pub(crate) fn read(page_id: usize) -> Option<Arc<Mutex<Page>>> {
    let id = Offset(page_id as u16);
    let cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    cache.get(&id).cloned().or_else(|| read_from_disk(page_id))
}

fn read_from_disk(page_id: usize) -> Option<Arc<Mutex<Page>>> {
    let file_offset = page_id * PAGE_SIZE_USIZE;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(INDEX_FILE)
        .unwrap();
    file.seek(SeekFrom::Start(file_offset as u64)).unwrap();
    let mut buffer = [0u8; PAGE_SIZE_USIZE];
    file.read(&mut buffer).unwrap();
    let new_page = Page::new_from(buffer);
    Some(Arc::new(Mutex::new(new_page)))
}

pub(crate) fn delete_index() {
    match fs::remove_file("index.000") {
        Ok(_) => println!("index.000 deleted."),
        Err(_) => println!("index.000 not found."),
    }

    match fs::remove_file("config") {
        Ok(_) => println!("config deleted."),
        Err(_) => println!("config not found."),
    }
}
