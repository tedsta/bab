use std::{cell::Cell, rc::Rc};

fn main() {
    let buffer_size = 64;
    let buffer_batch_count = 4;
    let buffer_batch_size = 8;
    let buffer_pool = bab::HeapBufferPool::new(buffer_size, buffer_batch_count, buffer_batch_size);
    assert_eq!(buffer_pool.total_buffer_count(), buffer_batch_count * buffer_batch_size);

    let writer_flush_queue = bab::WriterFlushQueue::new();
    let writer  = bab::Writer::new(buffer_pool.clone(), writer_flush_queue.clone(), 0);

    std::thread::spawn(move || {
        pollster::block_on(async {
            let mut write_buf = writer.reserve(7).await;
            write_buf[..].copy_from_slice(b"hello, ");
            let _: bab::Packet = write_buf.into();

            let mut write_buf = writer.reserve(5).await;
            write_buf[..].copy_from_slice(b"world");
            let _: bab::Packet = write_buf.into();

            writer.flush();

            std::thread::sleep(std::time::Duration::from_secs(1));

            let mut write_buf = writer.reserve(1).await;
            write_buf[..].copy_from_slice(b"!");
            let _: bab::Packet = write_buf.into();

            writer.flush();
        });
    });

    let received_bytes = Rc::new(Cell::new(0));
    let receiver = &mut bab::WriteFlusher::new(writer_flush_queue, {
        let received_bytes = received_bytes.clone();
        move |flush_buf| {
            received_bytes.set(received_bytes.get() + flush_buf.len());
            println!("Flushed bytes: '{}'", std::str::from_utf8(&flush_buf[..]).unwrap());
        }
    });
    pollster::block_on(async move {
        while received_bytes.get() < b"hello, world!".len() {
            receiver.flush().await;
            println!("{}", received_bytes.get());
        }
    });
}
