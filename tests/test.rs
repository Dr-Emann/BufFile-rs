extern crate buf_file;
extern crate rand;
extern crate tempfile;

use tempfile::tempfile;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::time::SystemTime;

use rand::{Rng, SeedableRng};
use rand::XorShiftRng;

use buf_file::BufFile;

#[test]
fn test_seek_past_end_read() {
    let mut test_file = BufFile::new(tempfile().unwrap()).unwrap();
    let mut buf = [0; 1024];
    test_file.seek(SeekFrom::Start(1)).unwrap();
    let read_count = test_file.read(&mut buf).unwrap();
    assert_eq!(read_count, 0);
}

#[test]
#[should_panic]
fn test_seek_end_error() {
    let mut test_file = BufFile::new(tempfile().unwrap()).unwrap();
    test_file.seek(SeekFrom::End(1)).unwrap();
}

#[test]
#[should_panic]
fn test_seek_current_error() {
    let mut test_file = BufFile::new(tempfile().unwrap()).unwrap();
    test_file.seek(SeekFrom::Current(1)).unwrap();
}

// This test verifies that the BufFile behaves exactly like a file when reading, writing, and seeking.
// It randomly seeks and writes data, and verifies everything is completely equal with the actual file.
#[test]
fn test_file_buffer() {
    struct CheckFiles<F: Read + Write + Seek> {
        real_file: F,
        buf_file: BufFile<F>,
    }

    impl<F: Read + Write + Seek> Seek for CheckFiles<F> {
        fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
            let real = self.real_file.seek(from);
            let buf = self.buf_file.seek(from);
            assert_eq!(real.as_ref().ok(), buf.as_ref().ok(), "Seek results should be equal");
            real
        }
    }

    impl<F: Read + Write + Seek> Read for CheckFiles<F> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut other_buf = vec![0u8; buf.len()];
            let real = self.real_file.read(buf);
            let buffered = self.buf_file.read(&mut other_buf);
            assert_eq!(real.as_ref().ok(), buffered.as_ref().ok(), "Read size should be equal");
            if let (&Ok(real_len), &Ok(buf_len)) = (&real, &buffered) {
                assert_eq!(&buf[..real_len], &other_buf[..buf_len], "Read data should be equal");
            }
            real
        }
    }

    impl<F: Read + Write + Seek> Write for CheckFiles<F> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let real = self.real_file.write(buf);
            let buffered = self.buf_file.write(buf);
            assert_eq!(real.as_ref().ok(), buffered.as_ref().ok(), "Read size should be equal");
            real
        }

        fn flush(&mut self) -> io::Result<()> {
            let real = self.real_file.flush();
            let buffered = self.buf_file.flush();
            assert_eq!(real.as_ref().ok(), buffered.as_ref().ok(), "Flush results should be equal");
            real
        }
    }
    let now = SystemTime::now();

    let test_file = tempfile().unwrap();
    let t = tempfile().unwrap();
    let test_buffile = BufFile::new(t).unwrap();
    let mut checker = CheckFiles {
        real_file: test_file,
        buf_file: test_buffile,
    };

    let mut rng = XorShiftRng::from_seed([0, 1, 377, 6712]);
    checker.write(&[0]).unwrap();
    for _ in 0..100 {
        for _ in 0..1000 {
            let current_len = checker.seek(SeekFrom::End(0)).unwrap();
            let x = rng.gen_range(0, current_len);
            checker.seek(SeekFrom::Start(x)).unwrap();
            let count = rng.gen_range(1, 100u32);
            for _ in 0..count {
                let byte = rng.gen::<u8>();
                checker.write(&[byte]).unwrap();
            }
        }
        let len = checker.seek(SeekFrom::End(0)).unwrap();
        checker.seek(SeekFrom::Start(0)).unwrap();
        for _ in 0..len {
            let mut buffer = [0];
            checker.read(&mut buffer).unwrap();
        }
    }

    match now.elapsed() {
        Ok(a) => println!("time for test_file_buffer: {:?}", a),
        Err(_) => panic!("Error measuring time.."),
    };
}
