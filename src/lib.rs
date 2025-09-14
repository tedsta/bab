#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(
    all(not(feature = "std")),
    feature(thread_local),
)]

#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc;

pub use buffer::BufferPtr;
pub use buffer_chain::{BufferChain, BufferChainDrain};
pub use buffer_pool::{BufferPool, HeapBufferPool};
pub use buffer_queue::{
    BufferQueueSender, BufferQueueReceiver, BufferQueueReceiveIterator,
    buffer_queue,
};
pub use buffer_writer::BufferWriter;
pub use packet::{Packet, SendPacket};
pub use framer::Framer;
pub use signal::Signal;
#[cfg(any(feature = "std", feature = "alloc"))]
pub use signal::SignalTree;
pub use writer::{
    DynWriter, LocalWriter, LocalWriterNoFlush, SharedWriter,
    Write, Writer,
};
pub use writer_flush::{Flush, WriterFlushReceiver, WriterFlushSender, new_writer_flusher};

mod buffer;
mod buffer_chain;
mod buffer_pool;
mod buffer_queue;
mod buffer_writer;
mod free_stack;
mod packet;
mod writer_flush;
mod framer;
mod signal;
mod writer;
