#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(
    all(not(feature = "std")),
    feature(thread_local),
)]

#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc;

pub use buffer::BufferPtr;
pub use buffer_pool::{BufferPool, HeapBufferPool};
pub use packet::{Packet, SendPacket};
pub use framer::Framer;
pub use signal::Signal;
pub use thread_local::ThreadLocal;
pub use writer::{
    DynWriter, LocalWriter, LocalWriterNoFlush, SharedWriter,
    Writer,
};
pub use writer_flush::{Flush, WriterFlushReceiver, WriterFlushSender, new_writer_flusher};

mod buffer;
mod buffer_pool;
mod free_stack;
mod packet;
mod writer_flush;
mod framer;
mod signal;
pub mod thread_id;
mod thread_local;
mod writer;
mod waiter_queue;
