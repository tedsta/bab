# Bab

*Disclaimer: This crate is experimental and uses quite a few unsafe blocks. It is not ready for production. If you find bugs / UB / race conditions, please do file an issue. Loom tests are a todo.*

Have you ever wanted to build your own bus? No? Well now you can!

![crab builds a bus](./images/crab-builds-a-bus.png)

Bab, short for "build a bus", is a crate that aims to provide foundational components for your app's message bus. Its core components are:

- An async `BufferPool` to allow quickly allocating and recycling buffers without any memory allocation after startup. With the current API the intention is that you normally shouldn't interact directly with a `BufferPool`.
- `Packet`, a handle to a message. Packets are backed by a region of a buffer, and multiple packets can be backed by the same buffer. Buffer references are managed - when all references to a buffer have been dropped, it is released back into the pool automatically.
- For creating and sending messages, `Writer` and `WriteFlusher`. Writers reserve space on automatically acquired buffers for messages. Written messages are made available to a `WriteFlusher` in batches by the writers it serves, for example to be flushed out on a TCP socket, with the batch and flush cadences fully under your control. Each written message is also returned to the caller in the form of a `Packet` to be circulated through the local application if desired.
- For ingesting incoming messages, `Framer`, which allows you to write bytes into a staging area to be framed into `Packet`s for your app's consumption.

Bab is opinionated and a bit quirky - it uses thread-local optimizations and generally assumes your app is thread-per-core. Its futures (for example those returned by `BufferPool::acquire`) are `!Send` and so cannot be used with a work-stealing executor such as Tokio's multi-threaded executor. You can use Tokio's `LocalSet` and futures-executor's `LocalPool`, for example, though.

`Packet`s are also `!Send`. To send a packet across threads, you must call `Packet::send` on it to get a `SendPacket`. Then at the receiving thread call `SendPacket::receive` to convert it back into a `Packet`.

## Roadmap

- Add `LocalWriter` - a `!Send` version of `Writer`.
- Safe interface for `BufferPool` / `BufferPtr`?
- Loom tests

## Why?

There are two somewhat related points that motivate bab - batching and thread-local optimizations.

### Batching

Batching is a common theme in bab, but my favorite example if it is in `Writer` / `WriteFlusher`. Multiple writers on multiple threads can be writing messages to the same underlying buffer, and all of those messages (potentially across multiple buffers) can be sent to the flusher in a single (fairly expensive) O(1) operation. Notice that flushing isn't the only thing happening in batches here - a single buffer can contain multiple messages, so you can very naturally pack multiple messages into a single outgoing packet (imagine you're using UDP), which can help you make better use of your network's MTU (jumbo frames, anyone?).

### Thread-local optimizations

On my x86 [System76 Darter Pro](https://system76.com/laptops/darter) laptop (is it free product placement?), criterion says that an uncontended `AtomicUsize::fetch_add` takes ~5.5 ns whereas a non-atomic `usize::wrapping_add` takes ~280 ps (1 - 2 clock cycles?), both with core affinity set. So in our microbenchmark, an atomic add is an order of magnitude more expensive than its non-atomic counterpart, even with the affected cache line only ever accessed from the same core. Concurrent data structures certainly are not free.

Bab offers hybrid concurrent data structures using thread-local optimizations to trade API convenience and implementation complexity for better performance.

An example of bab's thread-local optimizations: the cost of cloning a `Packet` is similar to the cost to cloning an `Rc` (very cheap), but you can still send the packet to another thread if desired (incurring an `Arc::clone`-like cost at that point).

Another example is that `BufferPool` maintains a thread-local cache of buffers, repopulating it in batches if it becomes empty, and releasing buffers back to the shared pool in batches if the local cache becomes too full. Further, the `WaiterQueue` structure, which provides the async-ness of both `BufferPool` and `Writer`, goes through some effort to maintain only a single thread-safe waiter registration per thread. All additional waiters on a given thread are registered in a thread-local list.

Criterion says that a stock `Vec::<u8>::with_capacity` and corresponding drop for a 1033 byte buffer takes ~32 ns while acquiring and releasing a buffer from a `bab::BufferPool` takes just over 8 ns. And the buffer pool has more functionality in that it also caps memory usage and notifies tasks waiting on buffers as buffers become available (though those code paths aren't exercised in this benchmark). In terms of functionality a more apt comparison would be 1) acquire semaphore permit 2) allocate buffer 3) release buffer 4) release semaphore permit.

## Performance

There are some limited benchmarks - you can play with `examples/writer_benchmark.rs` which has some configurable dimensions. You can also look at `benches/` which has a benchmark for `Framer` and `BufferPool`. I'll update this section with some concrete numbers in the future. But if you structure your bus in a scalable way (don't have all threads using the same `Writer` if raw throughput is what you're after), you should have no problem getting 1M - 10M messages per second per core.

## License

MIT