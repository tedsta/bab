use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn set_core_affinity(_: &mut Criterion) {
    core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
}

fn fetch_add_x1000(c: &mut Criterion) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    c.bench_function("fetch_add_x1000", |b| {
        b.iter(|| {
            let x = AtomicUsize::new(0);
            let prev = x.fetch_add(1, Ordering::Relaxed);
            black_box(prev);
        })
    });
}

fn wrapping_add_x1000(c: &mut Criterion) {
    c.bench_function("wrapping_add_x1000", |b| {
        b.iter(|| {
            let mut x = 0usize;
            black_box(&mut x);
            x = x.wrapping_add(1);
            black_box(&mut x);
        })
    });
}

fn malloc_free_vec1033_x1000(c: &mut Criterion) {
    c.bench_function("malloc_free_vec1033_x1000", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let mut b = Vec::<u8>::with_capacity(1033);
                black_box(&mut b);
            }
        })
    });
}

fn buffer_pool_acquire_release_1033_x1000(c: &mut Criterion) {
    let buffer_pool = bab::HeapBufferPool::new(1033, 64, 64);

    c.bench_function("buffer_pool_acquire_release_1033_x1000", |b| {
        b.iter(|| {
            pollster::block_on(async {
                for _ in 0..1000 {
                    let buffer = buffer_pool.acquire().await;
                    unsafe {
                        buffer_pool.release(buffer);
                    }
                }
            });
        })
    });
}

fn framer_x1000(c: &mut Criterion) {
    let write_payload = (0..16).collect::<Vec<_>>();

    let buffer_pool = bab::HeapBufferPool::new(16, 8, 1024);
    let mut framer = bab::Framer::new(buffer_pool);

    c.bench_function("framer_x1000", |b| {
        b.iter(|| {
            pollster::block_on(async {
                for _ in 0..1000 {
                    let buf: &mut [u8] = framer.write().await;
                    buf[..write_payload.len()].copy_from_slice(&write_payload);
                    framer.commit(write_payload.len());

                    let _packet = if framer.remaining_on_buffer() < write_payload.len() {
                        framer.next_buffer().unwrap()
                    } else {
                        framer.finish_frame().unwrap()
                    };
                }
            })
        })
    });
}

fn clone_bytes_1033(c: &mut Criterion) {
    let bytes = bytes::BytesMut::zeroed(1033).freeze();

    c.bench_function("clone_bytes_1033", |b| {
        b.iter(|| {
            black_box(bytes.clone());
        })
    });
}

fn clone_rc_1033(c: &mut Criterion) {
    let bytes = std::rc::Rc::new([0u8; 1033]);

    c.bench_function("clone_rc_1033", |b| {
        b.iter(|| {
            black_box(bytes.clone());
        })
    });
}

fn clone_packet_1033(c: &mut Criterion) {
    let buffer_pool = bab::HeapBufferPool::new(1033, 2, 2);
    let mut framer = bab::Framer::new(buffer_pool);

    let _buf = framer.try_write().unwrap();
    framer.commit(1033);
    let packet = framer.next_buffer().unwrap();

    c.bench_function("clone_packet_1033", |b| {
        b.iter(|| {
            black_box(packet.clone());
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        set_core_affinity,
        wrapping_add_x1000, fetch_add_x1000,
        framer_x1000,
        malloc_free_vec1033_x1000,
        buffer_pool_acquire_release_1033_x1000,
        clone_bytes_1033,
        clone_rc_1033,
        clone_packet_1033,
}
criterion_main!(benches);
