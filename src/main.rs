extern crate alloc;
extern crate core;

mod paging;
mod types;
mod errors;
mod btree;
mod io;
mod config;

fn main() {
    println!("Hello, world!");
    let _ = paging::Page::new_inner();
}
