use crate::{
    buffer::BufferPtr,
    packet::Packet,
    HeapBufferPool,
};

pub struct Framer {
    buffer_pool: HeapBufferPool,
    write_cursor: Option<FramerCursor>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct FramerCursor {
    buffer: BufferPtr,
    start: usize,
    end: usize,
    buffer_done: bool,
}

impl Framer {
    pub fn new(buffer_pool: HeapBufferPool) -> Self {
        Self {
            buffer_pool,
            write_cursor: None,
        }
    }

    // Returned WriteHandle commits the bytes when it is dropped.
    pub async fn write(&mut self) -> &mut [u8] {
        let write_cursor =
            // XXX If only there were a Option::get_or_insert_with_async
            match &mut self.write_cursor {
                Some(write_cursor) => write_cursor,
                write_cursor @ None => {
                    // Get next buffer
                    let buffer = self.buffer_pool.acquire().await;
                    *write_cursor = Some(FramerCursor {
                        buffer,
                        start: 0,
                        end: 0,
                        buffer_done: false,
                    });
                    write_cursor.as_ref().unwrap()
                }
            };

        let offset = write_cursor.end;

        unsafe {
            core::slice::from_raw_parts_mut(
                write_cursor.buffer.data().add(offset),
                self.buffer_pool.buffer_size() - offset,
            )
        }
    }

    pub fn remaining_on_buffer(&self) -> usize {
        let Some(write_cursor) = self.write_cursor.as_ref() else {
            // XXX is this what we want? Maybe should return None to make it explicit that we don't
            // have a buffer yet?
            return self.buffer_pool.buffer_size();
        };

        self.buffer_pool.buffer_size() - write_cursor.end
    }

    pub fn commit<'a>(&mut self, len: usize) {
        let Some(write_cursor) = self.write_cursor.as_mut() else {
            panic!("Framer::commit called without initial write on buffer.");
        };

        write_cursor.end += len;
    }

    pub fn finish_frame<'a>(&mut self) -> Option<Packet> {
        let write_cursor = self.write_cursor.as_mut()?;

        if write_cursor.end != write_cursor.start {
            let packet = Self::produce_packet(*write_cursor);
            write_cursor.start = write_cursor.end;
            Some(packet)
        } else {
            None
        }
    }

    pub fn next_buffer<'a>(&mut self) -> Option<Packet> {
        let mut write_cursor = self.write_cursor.take()?;

        if write_cursor.end != write_cursor.start {
            write_cursor.buffer_done = true;

            Some(Self::produce_packet(write_cursor))
        } else {
            // No new messages were written since the last call to `finish_frame` - decrement the
            // reference count on the current buffer.
            if write_cursor.start == 0 {
                // No messages were written on this buffer at all, so the reference count was never
                // initialized.
                unsafe { write_cursor.buffer.initialize_rc(1, 0, 0); }
            }

            unsafe { write_cursor.buffer.release_ref(1); }

            None
        }
    }

    fn produce_packet(written: FramerCursor) -> Packet {
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
        if written.start == 0 {
            if written.buffer_done {
                // Scenario 1
                unsafe { written.buffer.initialize_rc(1, 0, 0); }
            } else {
                // Scenario 2
                unsafe { written.buffer.initialize_rc(2, 0, 0); }
            }
        } else if !written.buffer_done {
            // Scenario 3
            unsafe { written.buffer.take_ref(1); }
        } else {
            // Scenario 4 - do nothing
        }

        Packet::new(
            written.buffer,
            written.start,
            written.end - written.start,
        )
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
