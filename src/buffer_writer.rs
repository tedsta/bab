use crate::{
    buffer::BufferPtr,
    HeapBufferPool,
};

pub struct BufferWriter {
    buffer_pool: HeapBufferPool,
    write_cursor: Option<BufferWriterCursor>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct BufferWriterCursor {
    buffer: BufferPtr,
    end: usize,
}

impl BufferWriter {
    pub fn new(buffer_pool: HeapBufferPool) -> Self {
        Self {
            buffer_pool,
            write_cursor: None,
        }
    }

    pub fn state(&self) -> Option<(BufferPtr, usize)> {
        let write_cursor = self.write_cursor?;
        Some((write_cursor.buffer, write_cursor.end))
    }

    pub async fn write(&mut self) -> &mut [u8] {
        let write_cursor =
            match &mut self.write_cursor {
                Some(write_cursor) => write_cursor,
                write_cursor @ None => {
                    // Get next buffer
                    let buffer = self.buffer_pool.acquire().await;
                    *write_cursor = Some(BufferWriterCursor {
                        buffer,
                        end: 0,
                    });
                    write_cursor.as_ref().unwrap()
                }
            };

        let offset = write_cursor.end;

        unsafe { write_cursor.buffer.slice_mut(offset..self.buffer_pool.buffer_size()) }
    }

    pub fn try_write(&mut self) -> Option<&mut [u8]> {
        let write_cursor =
            match &mut self.write_cursor {
                Some(write_cursor) => write_cursor,
                write_cursor @ None => {
                    // Get next buffer
                    let buffer = self.buffer_pool.try_acquire()?;
                    *write_cursor = Some(BufferWriterCursor {
                        buffer,
                        end: 0,
                    });
                    write_cursor.as_ref().unwrap()
                }
            };

        let offset = write_cursor.end;

        Some(unsafe {
            write_cursor.buffer.slice_mut(offset..self.buffer_pool.buffer_size())
        })
    }

    pub fn remaining_on_buffer(&self) -> usize {
        let Some(write_cursor) = self.write_cursor.as_ref() else {
            // XXX is this what we want? Maybe should return None to make it explicit that we don't
            // have a buffer yet?
            return self.buffer_pool.buffer_size();
        };
        self.buffer_pool.buffer_size() - write_cursor.end
    }

    pub fn is_empty(&self) -> bool {
        let Some(write_cursor) = self.write_cursor.as_ref() else {
            return true;
        };
        write_cursor.end == 0
    }

    pub fn commit(&mut self, len: usize) {
        let Some(write_cursor) = self.write_cursor.as_mut() else {
            panic!("BufferWriter::commit called without initial write on buffer.");
        };
        write_cursor.end += len;
    }

    #[inline]
    pub fn next_buffer(&mut self) -> Option<(BufferPtr, usize)> {
        let write_cursor = self.write_cursor.take()?;
        Some((write_cursor.buffer, write_cursor.end))
    }
}

impl Drop for BufferWriter {
    fn drop(&mut self) {
        // If we're holding onto a buffer, release it.
        self.next_buffer();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_buffer_writer() {
        use crate::HeapBufferPool;

        let buffer_pool = HeapBufferPool::new(16, 4, 4);
        let mut buffer_writer = BufferWriter::new(buffer_pool.clone());

        for _ in 0..32 {
            let buf: &mut [u8] = pollster::block_on(buffer_writer.write());
            buf[..5].copy_from_slice(b"hello");
            buffer_writer.commit(5);

            let buf: &mut [u8] = pollster::block_on(buffer_writer.write());
            buf[..5].copy_from_slice(b"world");
            buffer_writer.commit(5);

            let (buffer, len) = buffer_writer.next_buffer().unwrap();
            assert_eq!(unsafe { buffer.slice(0..len) }, b"helloworld");

            unsafe { buffer_pool.release(buffer); }
        }
    }
}
