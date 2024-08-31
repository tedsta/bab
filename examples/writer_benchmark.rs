use std::{
    cell::Cell,
    rc::Rc,
};

use core_affinity::CoreId;

fn main() {
    let message_size = 60;
    let write_payload: Vec<u8> = (0u16..message_size as _).map(|i| (i % 200) as u8).collect();
    let batch_size = 1000;

    let thread_count = 2;
    let iter_count = 30000000;

    let flushed_bytes = Rc::new(Cell::new(0));

    let buffer_size = 1350;
    let buffer_pool = bab::HeapBufferPool::new(buffer_size, 128, 256);
    let writer_flush_queue = bab::WriterFlushQueue::new();
    let writer = bab::SharedWriter::new(buffer_pool.clone(), writer_flush_queue.clone(), 0);
    let mut receiver = bab::WriteFlusher::new(writer_flush_queue.clone());

    for thread_id in 0..thread_count {
        //let writer = writer.clone(); // writers all using same buffer
        let writer_flush_queue = writer_flush_queue.clone();
        let buffer_pool = buffer_pool.clone();
        let write_payload = write_payload.clone();
        std::thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: thread_id + 1 });

            // each writer gets its own buffer
            let writer = bab::LocalWriter::new(
                buffer_pool.clone(),
                writer_flush_queue.clone(),
                0,
            );

            let writer = writer.to_dyn();

            let _buffer_pool_thread_guard = buffer_pool.register_thread();

            let mut sent_messages = 0;
            pollster::block_on(async {
                for _ in 0..(iter_count / batch_size / thread_count) {
                    for _ in 0..batch_size {
                        let mut write_buf = writer.reserve(write_payload.len()).await;
                        write_buf[..].copy_from_slice(&write_payload);
                        let _: bab::Packet = write_buf.into();

                        sent_messages += 1;
                    };

                    writer.flush();
                }
            });

            println!("Writer thread finished {}", thread_id);
        });
    }
    drop(writer);

    core_affinity::set_for_current(CoreId { id: 0 });

    let _buffer_pool_thread_guard = buffer_pool.register_thread();

    let now = std::time::Instant::now();

    let receiver = &mut receiver;
    pollster::block_on(async move {
        let expected_message_count =
            (iter_count / batch_size / thread_count) * (batch_size * thread_count);

        let mut next_progress = 0;
        while flushed_bytes.get() < expected_message_count * message_size {
            for flush in receiver.flush().await {
                flushed_bytes.set(flushed_bytes.get() + flush.len());

                for i in 0..flush.len() / message_size {
                    let start = i * message_size;
                    assert_eq!(&flush[start..start + message_size], &write_payload);
                }
            }

            let received_messages = flushed_bytes.get() / message_size;
            if received_messages >= next_progress {
                println!(
                    "Progress {:.2}%",
                    (received_messages as f64 / expected_message_count as f64) * 100.0,
                );
                next_progress += expected_message_count / 100;
            }
        }
    });

    let ns_per_iter = now.elapsed().as_nanos() / iter_count as u128;

    println!("ns per message flush: {}", ns_per_iter);
}
