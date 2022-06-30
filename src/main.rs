use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tempdir::TempDir;

/// There are 3 async tasks cooperating with each other.
/// - Write task writes data to file, update write_offset and notify flush task
/// - Flush task waits for write finishes, load write_offset, flush file and update flush_offset to write_offset
/// - Read task waits for flush finishes, load flush_offset as the persisted file length, read the file region [0, flush_offset) and checks if data read matches data written.
fn test() {
    let dir = TempDir::new("file-test").unwrap();
    let file_path = dir.path().join("data").to_string_lossy().to_string();
    println!("file: {}", file_path);

    let data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    let data_len = data.len();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(&file_path)
        .unwrap();

    // data offset written to file but not yet flushed.
    let data_written_offset = Arc::new(AtomicUsize::new(0));
    // data offset flushed.
    let data_flush_offset = Arc::new(AtomicUsize::new(0));

    // write data to file and notify flush thread.
    file.write_all(data.as_bytes()).unwrap();
    data_written_offset.store(data_len, Ordering::SeqCst); // update written offset
    println!("write finish: {}", data_len);

    let written_offset = data_written_offset.clone().load(Ordering::SeqCst);
    file.try_clone().unwrap().sync_all().unwrap();
    data_flush_offset.clone().store(written_offset, Ordering::SeqCst);
    println!("flush: {}", written_offset);

    let flush_offset_cloned = data_flush_offset.clone();
    let mut file_cloned = file.try_clone().unwrap();
    let flush_offset = flush_offset_cloned.load(Ordering::SeqCst);
    // checks if file length in metadata matches flush offset.
    assert_eq!(flush_offset as u64, file_cloned.metadata().unwrap().len());
    let mut content = String::new();
    file_cloned.seek(SeekFrom::Start(0)).unwrap();
    file_cloned.read_to_string(&mut content).unwrap();
    assert_eq!(data, &content);
}

fn main() {
    coredump::register_panic_handler().unwrap();
    test();
}