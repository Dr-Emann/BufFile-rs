use std::io::prelude::*;
use std::io::{self, ErrorKind, SeekFrom};
use std::collections::HashMap;
use std::cmp;

use lru_cache::LruCache;

/// Slab size MUST be a power of 2!
const SLAB_SIZE: usize = 512*1024; // Change this number to change the SLAB_SIZE (currently @ 512kb)

/// Used to turn a file index into an array index (since SLAB_SIZE is a power of two,
/// subtracting one from it will yield all ones, and anding it with a number will
/// yield only the lowest n bits, where SLAB_SIZE = 2^n
const SLAB_MASK: u64 = SLAB_SIZE as u64 - 1;

const DEFAULT_CAPACITY: usize = 16;

/// A struct representing a section of a file
struct Slab {
    /// The data
    data: Box<[u8]>,
    bytes_used: usize,
    /// Has the slab been written to, and not written to disk?
    dirty: bool
}

impl Slab {
    /// Creates a new slab, drawing it's data from the given file at the given location
    /// Location should be at the beginning of a slab (e.g. a multiple of `SLAB_SIZE`)
    fn new() -> Slab {
        let data = if cfg!(debug_assertions) {
            vec![0u8; SLAB_SIZE]
        } else {
            let mut vec = Vec::with_capacity(SLAB_SIZE);
            unsafe {
                vec.set_len(SLAB_SIZE);
            }
            vec
        };
        Slab {
            data: data.into_boxed_slice(),
            bytes_used: 0,
            dirty: false
        }
    }

    fn flush<F: Write + Seek>(&mut self, f: &mut F, offset: u64) -> io::Result<()> {
        if self.dirty {
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&self.data)?;
            self.dirty = false;
        }
        Ok(())
    }
}

pub struct BufFile<F: Write + Read + Seek> {
    slabs: LruCache<usize, Slab>,
    /// The file to be written to and read from
    file: Option<F>,
    /// Represents the current location of the cursor.
    /// This does not reflect the actual location of the cursor in the file.
    cursor: u64,
}

impl<F: Write + Read + Seek> BufFile<F> {
    /// Creates a new BufFile.
    pub fn new(file: F) -> io::Result<BufFile<F>> {
        Self::with_capacity(file, DEFAULT_CAPACITY)
    }

    /// Creates a new BufFile with the specified number of slabs.
    pub fn with_capacity(mut file: F, capacity: usize) -> io::Result<BufFile<F>> {
        let current = file.seek(SeekFrom::Current(0))?;
        Ok(BufFile {
            slabs: LruCache::new(capacity),
            file: Some(file),
            cursor: current,      // Since the cursor is at the start of the file
        })
    }

    /// Returns the underlying Read + Write + Sync struct after writing to disk.
    pub fn into_inner(mut self) -> io::Result<F> {
        self.flush()?;
        Ok(self.file.take().unwrap())
    }

    /*
    /// Change the number of slabs to the desired number. If there are more slabs
    /// currently loaded than `num_slabs`, then the least frequently used slab(s)
    /// will be removed until it is equal. Every removed slab gets written to disk,
    /// creating the possibility for I/O errors.
    pub fn set_slabs(&mut self, num_slabs: usize) -> Result<(), Error> {
        // There isn't anything logical to actually do here, so just return
        if num_slabs == 0 { return Ok(()) }
        if num_slabs >= self.dat.len() {
            self.slabs = num_slabs;
            return Ok(())
        }
        while self.dat.len() > num_slabs {
            let mut min = 0;
            for i in 0..self.slabs {
                if self.dat[min].uses == 1 {
                    min = i;
                    // The minimum number of reads is 1, so if we encounter 1 just break.
                    break;
                }
                if self.dat[min].uses > self.dat[i].uses {
                    min = i;
                }
            }
            self.dat[min].write(self.file.as_mut().unwrap())?;
            let _ = self.dat.swap_remove(min);
        }
        self.slabs = num_slabs;
        Ok(())
    }
    */

    /// Returns the current cursor_loc
    pub fn cursor_loc(&self) -> u64 {
        self.cursor
    }

    /// Find the existing slab, or retrieve it manually
    fn fetch_slab(&mut self, idx: usize) -> io::Result<(&mut Slab, &mut F)> {
        if self.slabs.contains_key(&idx) {
            let slab = self.slabs.get_mut(&idx).unwrap();
            return Ok((slab, self.file.as_mut().unwrap()));
        }
        self.add_slab(idx)
    }

    /// Adds a slab to the BufFile, if it isn't already present. It will write
    /// the least frequently used slab to disk and load the new one into self.dat,
    /// then return Ok(index), index being an index for self.dat.
    fn add_slab(&mut self, idx: usize) -> io::Result<(&mut Slab, &mut F)> {
        let mut file = self.file.as_mut().unwrap();
        let slab = if self.slabs.len() == self.slabs.capacity() {
            let (old_idx, mut old_slab) = self.slabs.remove_lru().expect("Capacity should never be 0");
            let old_offset = old_idx as u64 * SLAB_SIZE as u64;
            old_slab.flush(&mut file, old_offset)?;
            old_slab.bytes_used = 0;
            old_slab
        } else {
            Slab::new()
        };
        self.slabs.insert(idx, slab);
        let slab = self.slabs.get_mut(&idx).expect("Value should exist, was just inserted");
        Ok((slab, file))
    }
}


fn idx_from_offset(offset: u64) -> usize {
    (offset / SLAB_SIZE as u64) as usize
}

impl<F: Write + Read + Seek> Read for BufFile<F> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let cursor = self.cursor;
        let idx = idx_from_offset(cursor);
        let slab_start = idx as u64 * SLAB_SIZE as u64;
        let cursor_offset = (cursor - slab_start) as usize;
        let len = {
            let (slab, file) = self.fetch_slab(idx)?;
            while cursor_offset >= slab.bytes_used {
                let bytes_read = file.read(&mut slab.data[slab.bytes_used..])?;
                if bytes_read == 0 {
                    break;
                }
                slab.bytes_used += bytes_read;
            }
            let len = slab.bytes_used - cursor_offset;
            buf[..len].copy_from_slice(&slab.data[cursor_offset..slab.bytes_used]);
            len
        };

        self.cursor += len as u64;
        Ok(len)
    }
}

impl<F: Write + Read + Seek> Write for BufFile<F> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let cursor = self.cursor;
        let idx = idx_from_offset(cursor);
        let slab_start = idx as u64 * SLAB_SIZE as u64;
        let cursor_offset = (cursor - slab_start) as usize;
        let len = {
            let (slab, file) = self.fetch_slab(idx)?;
            slab.dirty = true;
            // we still need to read up until the write location
            while cursor_offset > slab.bytes_used {
                let bytes_read = file.read(&mut slab.data[slab.bytes_used..cursor_offset])?;
                if bytes_read == 0 {
                    break;
                }
                slab.bytes_used += bytes_read;
            }
            let len = cmp::min(buf.len(), SLAB_SIZE - cursor_offset);
            slab.data[cursor_offset..cursor_offset + len].copy_from_slice(&buf[..len]);
            len
        };
        self.cursor += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self.file.as_mut().unwrap();
        for (&idx, slab) in self.slabs.iter_mut() {
            let offset = idx as u64 * SLAB_SIZE as u64;
            slab.flush(&mut file, offset)?;
        }
        file.flush()
    }
}

impl<F: Write + Read + Seek> Seek for BufFile<F> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(x) => {
                self.cursor = x;
                self.cursor
            },
            SeekFrom::End(_) => {
                let file = self.file.as_mut().unwrap();
                let cursor = file.seek(pos)?;
                self.cursor = cursor;
                cursor
            },
            SeekFrom::Current(x) => {
                let cur = self.cursor;

                let cursor =
                    if x < 0 { cur - (-x) as u64 }
                    else { cur - x as u64 };
                self.cursor = cursor;
                cursor
            }
        };
        Ok(new_pos)
    }
}

impl<F: Read + Write + Seek> Drop for BufFile<F> {
    /// Write all of the slabs to disk before closing the file.
     fn drop(&mut self) {
         if self.file.is_none() { return }
         let _ = self.flush();
     }
}
