use std::io::SeekFrom;
use tempdir::TempDir;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// There are 3 async tasks cooperating with each other.
/// - Write task writes data to file, update write_offset and notify flush task
/// - Flush task waits for write finishes, load write_offset, flush file and update flush_offset to write_offset
/// - Read task waits for flush finishes, load flush_offset as the persisted file length, read the file region [0, flush_offset) and checks if data read matches data written.
async fn test() {
    let dir = TempDir::new("file-test").unwrap();
    let file_path = dir.path().join("data").to_string_lossy().to_string();
    println!("file: {}", file_path);

    let data = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    let data_len = data.len();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(&file_path)
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
    let write_offset_cloned = data_written_offset.clone();
    let flush_offset_cloned = data_flush_offset.clone();
    let file_cloned = file.try_clone().await.unwrap();
    tokio::spawn(async move {
        write_finish_notify_cloned.notified().await;
        let written_offset = write_offset_cloned.load(Ordering::SeqCst);
        file_cloned.sync_all().await.unwrap();
        flush_offset_cloned.store(written_offset, Ordering::SeqCst);
        println!("flush: {}", written_offset);
        flush_finish_notify_cloned.notify_one();
    });

    // start read task
    let flush_notify_cloned = flush_finish_notify.clone();
    let flush_offset_cloned = data_flush_offset.clone();
    let mut file_cloned = file.try_clone().await.unwrap();
    let handle = tokio::spawn(async move {
        flush_notify_cloned.notified().await;
        let flush_offset = flush_offset_cloned.load(Ordering::SeqCst);
        // checks if file length in metadata matches flush offset.
        assert_eq!(flush_offset as u64, file_cloned.metadata().await.unwrap().len());
        let mut content = String::new();
        file_cloned.seek(SeekFrom::Start(0)).await.unwrap();
        file_cloned.read_to_string(&mut content).await.unwrap();
        assert_eq!(data, &content);
    });

    // write data to file and notify flush thread.
    file.write_all(data.as_bytes()).await.unwrap();
    data_written_offset.store(data_len, Ordering::SeqCst); // update written offset
    println!("write finish: {}", data_len);
    write_finish_notify.notify_one();

    handle.await.unwrap();
}

#[tokio::main]
async fn main() {
    test().await;
}