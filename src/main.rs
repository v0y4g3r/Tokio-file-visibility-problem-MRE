use memmap2::MmapOptions;
use tempdir::TempDir;
use tokio::io::AsyncWriteExt;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Release};

/// There are 3 async tasks cooperating with each other.
/// - Write task writes data to file, update write_offset and notify flush task
/// - Flush task waits for write finishes, load write_offset, flush file and update flush_offset to write_offset
/// - Mmap task waits for flush finishes, load flush_offset as the persisted file length, mmap the file region [0, flush_offset) and checks if data read matches data written.
async fn test() {
    let dir = TempDir::new("file-test").unwrap();
    let dir_str = dir.path().to_string_lossy().to_string();
    println!("dir: {}", dir_str);

    let data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    let data_len = data.len();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(dir.path().join("data"))
        .await
        .unwrap();

    // data offset written to file but not yet flushed.
    let data_written_offset = Arc::new(AtomicUsize::new(0));
    // data offset flushed.
    let data_flush_offset = Arc::new(AtomicUsize::new(0));

    // condition variables
    let flush_finish_notify = Arc::new(tokio::sync::Notify::new());
    let write_finish_notify = Arc::new(tokio::sync::Notify::new());
    let write_finish_notify_cloned = write_finish_notify.clone();

    let flush_finish_notify_cloned = flush_finish_notify.clone();
    let data_written_ofs_1 = data_written_offset.clone();
    let data_flush_offset_1 = data_flush_offset.clone();
    let file_cloned = file.try_clone().await.unwrap();
    tokio::spawn(async move {
        write_finish_notify_cloned.notified().await;
        let written_offset = data_written_ofs_1.load(Acquire);
        file_cloned.sync_all().await.unwrap();
        data_flush_offset_1.store(written_offset, Release);
        println!("flush: {}", written_offset);
        flush_finish_notify_cloned.notify_one();
    });

    // start mmap read task
    let flush_notify_cloned2 = flush_finish_notify.clone();
    let flush_offset_2 = data_flush_offset.clone();
    let file_cloned_2 = file.try_clone().await.unwrap();
    let handle = tokio::spawn(async move {
        flush_notify_cloned2.notified().await;
        let flush_offset = flush_offset_2.load(Acquire);
        println!("mmap:{}", flush_offset);
        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .len(flush_offset)
                .populate()
                .map(&file_cloned_2)
                .unwrap()
        };
        assert_eq!(data.as_bytes(), &mmap[0..data_len]);
    });

    // write data to file and notify flush thread.
    file.write_all(data.as_bytes()).await.unwrap();
    data_written_offset.store(data_len, Release); // update written offset
    println!("write finish: {}", data_len);
    write_finish_notify.notify_one();

    // wait mmap finish.
    handle.await.unwrap();
}

#[tokio::main]
async fn main() {
    test().await;
}