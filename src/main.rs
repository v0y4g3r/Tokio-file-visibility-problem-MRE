use memmap2::MmapOptions;
use tempdir::TempDir;
use tokio::io::AsyncWriteExt;
use std::sync::Arc;

async fn test() {
    let dir = TempDir::new("file-test").unwrap();
    let dir_str = dir.path().to_string_lossy().to_string();
    println!("dir: {}", dir_str);

    let data = "/home/lei/Workspace/greptimedb/target/debug/deps/log_store-224a87479e045fe6fresh:false}";
    let data_len = data.len();
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(dir.path().join("data"))
        .await
        .unwrap();



    let file_cloned = file.try_clone().await.unwrap();
    let flush_finish_notify = Arc::new(tokio::sync::Notify::new());
    let write_finish_notify = Arc::new(tokio::sync::Notify::new());
    let write_finish_notify_cloned = write_finish_notify.clone();

    let flush_finish_notify_cloned = flush_finish_notify.clone();

    tokio::spawn(async move {
        write_finish_notify_cloned.notified().await;
        file_cloned.sync_all().await.unwrap();
        println!("flush");
        flush_finish_notify_cloned.notify_one();
    });

    file.write_all(data.as_bytes()).await.unwrap();
    println!("write");
    write_finish_notify.notify_one();
    flush_finish_notify.notified().await;

    let mmap = unsafe {
        MmapOptions::new()
            .offset(0)
            .len(data_len)
            .populate()
            .map(&file)
            .unwrap()
    };
    assert_eq!(data.as_bytes(), &mmap[0..data_len]);
}

#[tokio::main]
async fn main() {
    test().await;
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_main() {}
}