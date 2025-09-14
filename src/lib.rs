#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(all(not(feature = "std")), feature(thread_local))]

#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc;

pub use buffer::BufferPtr;
pub use buffer_chain::{BufferChain, BufferChainDrain};
pub use buffer_pool::{BufferPool, HeapBufferPool};
pub use buffer_queue::{
    BufferQueueReceiveIterator, BufferQueueReceiver, BufferQueueSender, buffer_queue,
};
pub use buffer_writer::BufferWriter;
pub use framer::Framer;
pub use packet::{Packet, SendPacket};
pub use signal::Signal;
#[cfg(any(feature = "std", feature = "alloc"))]
pub use signal::SignalTree;
pub use writer::{DynWriter, LocalWriter, LocalWriterNoFlush, SharedWriter, Write, Writer};
pub use writer_flush::{Flush, WriterFlushReceiver, WriterFlushSender, new_writer_flusher};

mod buffer;
mod buffer_chain;
mod buffer_pool;
mod buffer_queue;
mod buffer_writer;
mod framer;
mod free_stack;
mod packet;
mod signal;
mod writer;
mod writer_flush;
