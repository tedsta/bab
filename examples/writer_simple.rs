fn main() {
    let buffer_size = 64;
    let pool_batch_count = 4;
    let pool_batch_size = 8;
    let buffer_pool = bab::HeapBufferPool::new(buffer_size, pool_batch_count, pool_batch_size);
    assert_eq!(buffer_pool.total_buffer_count(), pool_batch_count * pool_batch_size);

    let (writer_flush_sender, mut writer_flush_receiver) = bab::new_writer_flusher();
    let writer_id = 42;
    let writer = bab::Writer::new_shared(buffer_pool, writer_flush_sender.clone(), writer_id);

    std::thread::spawn(move || {
        pollster::block_on(async {
            let mut write_buf = writer.reserve(7).await;
            write_buf[..].copy_from_slice(b"hello, ");
            let _: bab::Packet = write_buf.into();

            let mut write_buf = writer.reserve(5).await;
            write_buf[..].copy_from_slice(b"world");
            drop(write_buf);

            writer.flush();

            std::thread::sleep(std::time::Duration::from_secs(1));

            let mut write_buf = writer.reserve(1).await;
            write_buf[..].copy_from_slice(b"!");
            drop(write_buf);

            writer.flush();
        });
    });

    let received_bytes = std::cell::Cell::new(0);
    pollster::block_on(async {
        while received_bytes.get() < b"hello, world!".len() {
            for flush in writer_flush_receiver.flush().await {
                assert_eq!(flush.writer_id(), writer_id);
                received_bytes.set(received_bytes.get() + flush.len());
                println!("Flushed bytes: '{}'", std::str::from_utf8(&flush[..]).unwrap());
            }
        }
    });
}
