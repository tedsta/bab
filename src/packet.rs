use core::{
    cell::Cell,
    mem::MaybeUninit,
};

use crate::buffer::BufferPtr;

pub struct Packet {
    buffer: BufferPtr,
    offset: Cell<u32>,
    len: Cell<u32>,
}

impl Packet {
    pub(crate) fn new(buffer: BufferPtr, offset: usize, len: usize) -> Self {
        Self {
            buffer,
            offset: Cell::new(offset as u32),
            len: Cell::new(len as u32),
        }
    }

    #[inline]
    pub fn send(self) -> SendPacket {
        let shared_rc_contribution = unsafe { self.buffer.send() };
        let packet = SendPacket {
            buffer: self.buffer,
            offset: self.offset.clone(),
            len: self.len.clone(),
            shared_rc_contribution,
        };

        core::mem::forget(self);

        packet
    }

    pub fn len(&self) -> usize { self.len.get() as usize }

    pub fn advance(&self, n: usize) {
        let n = n as u32;
        assert!(self.len.get() >= n);
        self.offset.set(self.offset.get() + n);
        self.len.set(self.len.get() - n);
    }

    pub fn shatter_into(self, sorted_offsets: &[usize], out: &mut [MaybeUninit<Self>]) {
        assert!(out.len() >= sorted_offsets.len());

        if sorted_offsets.len() > 1 {
            unsafe { self.buffer.take_ref(sorted_offsets.len() as u32 - 1); }
        }

        let regions = (0..sorted_offsets.len() - 1)
            .map(|i| [sorted_offsets[i], sorted_offsets[i + 1]]);
        for ([offset, end], out) in regions.zip(out.iter_mut()) {
            let offset = offset as u32;
            let end = end as u32;
            out.write(Packet {
                buffer: self.buffer,
                offset: Cell::new(self.offset.get() + offset),
                len: Cell::new(end - offset),
            });
        }

        let last = sorted_offsets.len() - 1;
        out[last].write(Packet {
            buffer: self.buffer,
            offset: Cell::new(self.offset.get() + sorted_offsets[last] as u32),
            len: Cell::new(self.len.get() - sorted_offsets[last] as u32),
        });

        core::mem::forget(self);
    }
}

impl core::ops::Deref for Packet {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.buffer.data().add(self.offset.get() as usize),
                self.len.get() as usize,
            )
        }
    }
}

impl AsRef<[u8]> for Packet {
    fn as_ref(&self) -> &[u8] { core::ops::Deref::deref(self) }
}

impl Clone for Packet {
    fn clone(&self) -> Self {
        unsafe { self.buffer.take_ref(1); }

        Self {
            buffer: self.buffer,
            offset: self.offset.clone(),
            len: self.len.clone(),
        }
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        unsafe { self.buffer.release_ref(1); }
    }
}

impl core::fmt::Debug for Packet {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Packet")
         .field("buffer", &unsafe { self.buffer.id() })
         .field("offset", &self.offset.get())
         .field("len", &self.len.get())
         .finish()
    }
}

pub struct SendPacket {
    buffer: BufferPtr,
    offset: Cell<u32>,
    len: Cell<u32>,
    shared_rc_contribution: u32,
}

unsafe impl Send for SendPacket { }

impl SendPacket {
    #[inline]
    pub fn receive(self) -> Packet {
        unsafe { self.buffer.receive(self.shared_rc_contribution) };
        let packet = Packet {
            buffer: self.buffer,
            offset: self.offset.clone(),
            len: self.len.clone(),
        };

        core::mem::forget(self);

        packet
    }

    pub fn len(&self) -> usize { self.len.get() as usize }

    pub fn advance(&self, n: usize) {
        let n = n as u32;
        assert!(self.len.get() >= n);
        self.offset.set(self.offset.get() + n);
        self.len.set(self.len.get() - n);
    }
}

impl core::ops::Deref for SendPacket {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            core::slice::from_raw_parts(
                self.buffer.data().add(self.offset.get() as usize),
                self.len.get() as usize,
            )
        }
    }
}

impl AsRef<[u8]> for SendPacket {
    fn as_ref(&self) -> &[u8] { core::ops::Deref::deref(self) }
}

impl Drop for SendPacket {
    fn drop(&mut self) {
        unsafe {
            self.buffer.receive(self.shared_rc_contribution);
            self.buffer.release_ref(1);
        }
    }
}

impl core::fmt::Debug for SendPacket {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SendPacket")
         .field("buffer", &unsafe { self.buffer.id() })
         .field("offset", &self.offset.get())
         .field("len", &self.len.get())
         .finish()
    }
}
