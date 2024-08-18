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
pub use write_flusher::{WriteFlusher, Flush};
pub use framer::Framer;
pub use thread_local::ThreadLocal;
pub use writer::{DynWriter, LocalWriter, SharedWriter, Writer, WriterFlushQueue};

mod buffer;
mod buffer_pool;
mod free_stack;
mod packet;
mod write_flusher;
mod framer;
pub mod thread_id;
mod thread_local;
mod writer;
mod waiter_queue;
