use std::{
    cell::Cell,
    rc::Rc,
};

use core_affinity::CoreId;

fn main() {
    use bab::{HeapBufferPool, WriterFactory, WriterFlushQueue, WriteFlusher};

    let message_size = 60;
    let write_payload: Vec<u8> = (0u16..message_size as _).map(|i| (i % 200) as u8).collect();
    let batch_size = 1000;

    let thread_count = 2;
    let iter_count = 30000000;

    let flushed_bytes = Rc::new(Cell::new(0));

    let buffer_size = 1350;
    let buffer_pool = HeapBufferPool::new(buffer_size, 32, 256);
    let writer_flush_queue = WriterFlushQueue::new();
    let sender_factory = WriterFactory::new(buffer_pool.clone(), writer_flush_queue.clone());
    let sender = sender_factory.new_writer(0);
    let mut receiver = WriteFlusher::new(writer_flush_queue, {
        let flushed_bytes = flushed_bytes.clone();
        let write_payload = write_payload.clone();
        move |flush| {
            flushed_bytes.set(flushed_bytes.get() + flush.len());

            for i in 0..flush.len() / message_size {
                let start = i * message_size;
                assert_eq!(&flush[start..start + message_size], &write_payload);
            }
        }
    });

    for thread_id in 0..thread_count {
        let sender = sender.clone(); // writers all using same buffer
        //let sender = sender_factory.new_writer(0); // each writer gets its own buffer
        let buffer_pool = buffer_pool.clone();
        let write_payload = write_payload.clone();
        std::thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: thread_id + 1 });

            let _buffer_pool_thread_guard = buffer_pool.register_thread();

            let mut sent_messages = 0;
            pollster::block_on(async {
                for _ in 0..(iter_count / batch_size / thread_count) {
                    for _ in 0..batch_size {
                        let mut buf = sender.reserve(write_payload.len()).await;
                        buf[..].copy_from_slice(&write_payload);
                        let _: bab::Packet = buf.into();

                        sent_messages += 1;
                    };

                    sender.flush();
                }
            });

            println!("Writer thread finished {}", thread_id);
        });
    }
    drop(sender_factory);
    drop(sender);

    core_affinity::set_for_current(CoreId { id: 0 });

    let _buffer_pool_thread_guard = buffer_pool.register_thread();

    let now = std::time::Instant::now();

    let receiver = &mut receiver;
    pollster::block_on(async move {
        let expected_message_count =
            (iter_count / batch_size / thread_count) * (batch_size * thread_count);

        let mut next_progress = 0;
        while flushed_bytes.get() < expected_message_count * message_size {
            receiver.flush().await;

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

    println!("ns per iter: {}", ns_per_iter);
}
