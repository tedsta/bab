use crate::{BufferWriter, HeapBufferPool, buffer::BufferPtr, packet::Packet};

pub struct Framer {
    buffer_writer: BufferWriter,
    frame_start: usize,
}

impl Framer {
    pub fn new(buffer_pool: HeapBufferPool) -> Self {
        Self {
            buffer_writer: BufferWriter::new(buffer_pool),
            frame_start: 0,
        }
    }

    #[inline]
    pub async fn write(&mut self) -> &mut [u8] {
        self.buffer_writer.write().await
    }

    #[inline]
    pub fn try_write(&mut self) -> Option<&mut [u8]> {
        self.buffer_writer.try_write()
    }

    #[inline]
    pub fn remaining_on_buffer(&self) -> usize {
        self.buffer_writer.remaining_on_buffer()
    }

    #[inline]
    pub fn commit(&mut self, len: usize) {
        self.buffer_writer.commit(len)
    }

    #[inline]
    pub fn finish_frame(&mut self) -> Option<Packet> {
        let (buffer, written_len) = self.buffer_writer.state()?;

        if written_len != self.frame_start {
            let packet = Self::produce_packet(buffer, self.frame_start, written_len, false);
            self.frame_start = written_len;
            Some(packet)
        } else {
            None
        }
    }

    #[inline]
    pub fn next_buffer(&mut self) -> Option<Packet> {
        let (buffer, written_len) = self.buffer_writer.next_buffer()?;

        if written_len != self.frame_start {
            let packet_start = self.frame_start;
            self.frame_start = 0;
            Some(Self::produce_packet(
                buffer,
                packet_start,
                written_len,
                true,
            ))
        } else {
            // No new messages were written since the last call to `finish_frame` - decrement the
            // reference count on the current buffer.
            if self.frame_start == 0 {
                // No messages were written on this buffer at all, so the reference count was never
                // initialized.
                unsafe {
                    buffer.initialize_rc(1, 0, 0);
                }
            }

            unsafe {
                buffer.release_ref(1);
            }

            None
        }
    }

    #[inline]
    fn produce_packet(
        buffer: BufferPtr,
        packet_start: usize,
        packet_end: usize,
        buffer_done: bool,
    ) -> Packet {
        // Four scenarios to handle when updating the buffer's reference count:
        // 1. It's the first and only message on the buffer - set the reference count to 1.
        // 2. It's the first message of potentially multiple on the buffer - set the reference count
        //    to 2. One for the new Packet and one for us since the buffer can still be written
        //    to and so can't be freed.
        // 3. It's not the first message on the buffer and we aren't switching buffers yet -
        //    increment the reference count by 1.
        // 4. It's the last of multiple messages on the buffer - don't modify the reference count.
        //    The decrement we'd do since we are switching buffers cancels out with the increment
        //    for the new message.
        if packet_start == 0 {
            if buffer_done {
                // Scenario 1
                unsafe {
                    buffer.initialize_rc(1, 0, 0);
                }
            } else {
                // Scenario 2
                unsafe {
                    buffer.initialize_rc(2, 0, 0);
                }
            }
        } else if !buffer_done {
            // Scenario 3
            unsafe {
                buffer.take_ref(1);
            }
        } else {
            // Scenario 4 - do nothing
        }

        unsafe { Packet::new(buffer, packet_start, packet_end - packet_start) }
    }
}

impl Drop for Framer {
    fn drop(&mut self) {
        // If we're holding onto a buffer, release it.
        self.next_buffer();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_framer() {
        use crate::HeapBufferPool;

        let buffer_pool = HeapBufferPool::new(16, 4, 4);
        let mut framer = Framer::new(buffer_pool);

        for _ in 0..32 {
            {
                let buf: &mut [u8] = pollster::block_on(framer.write());
                buf[..5].copy_from_slice(b"hello");
            }

            framer.commit(5);
            let message = framer.next_buffer().unwrap();
            assert_eq!(&*message, b"hello");
        }
    }
}
