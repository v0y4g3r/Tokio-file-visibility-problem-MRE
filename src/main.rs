use memmap2::MmapOptions;
use tempdir::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::spawn;
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

    file.write_all(data.as_bytes()).await.unwrap();

    let file_cloned = file.try_clone().await.unwrap();
    let notify = Arc::new(tokio::sync::Notify::new());

    let notify_cloned = notify.clone();
    tokio::spawn(async move {
        file_cloned.sync_all().await.unwrap();
        notify_cloned.notify_one();
    });

    notify.notified().await;

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