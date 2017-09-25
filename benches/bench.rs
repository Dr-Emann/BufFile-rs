#![feature(test)]

extern crate buf_file;
extern crate test;
extern crate tempfile;
extern crate rand;
extern crate seek_bufread;

mod prelude {
    pub use std::io::{ Seek, Write, Read, SeekFrom, BufWriter, BufReader };

    pub use test::Bencher;

    pub use tempfile::tempfile;

    pub use rand::{Rng, SeedableRng, XorShiftRng};
    pub use rand::distributions::IndependentSample;

    pub use buf_file::BufFile;
    pub use std::fs::File;
}

use prelude::*;


const FILE_SIZE: usize = 16 * 1024 * 1024;

mod sequential_read {
    use prelude::*;
    use tempfile;
    use FILE_SIZE;

    fn test<R: Read, F: FnMut(File) -> R>(b: &mut Bencher, mut f: F) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.set_len(FILE_SIZE as u64).unwrap();
        file.flush().unwrap();
        let mut big_buffer = vec![0u8; FILE_SIZE];
        b.iter(|| {
            let mut test_file = f(file.reopen().unwrap());
            for i in 0..FILE_SIZE / 1024 {
                test_file.read_exact(&mut big_buffer[i * 1024..(i+1) * 1024]).unwrap();
            }
        });
        b.bytes = FILE_SIZE as _;
    }

    #[bench]
    fn read_16_mb_buf_file(b: &mut Bencher) {
        test(b, |f| BufFile::new(f).unwrap());
    }

    #[bench]
    fn read_16_mb_bufreader(b: &mut Bencher) {
        test(b, |f| BufReader::new(f));
    }

    #[bench]
    fn read_16_mb_no_buf(b: &mut Bencher) {
        test(b, |f| f);
    }
}

mod sequential_write {
    use prelude::*;
    use FILE_SIZE;

    fn test<W: Write, F: FnMut(File) -> W>(b: &mut Bencher, mut f: F) {
        let kb = vec![0u8; 1024];
        b.iter(|| {
            let mut test_file = f(tempfile().unwrap());
            for _ in 0..FILE_SIZE / 1024 {
                test_file.write_all(&kb).unwrap();
            }
        });
        b.bytes = FILE_SIZE as _;
    }

    #[bench]
    fn write_16_mb_buf_file(b: &mut Bencher) {
        test(b, |f| BufFile::new(f).unwrap());
    }


    #[bench]
    fn write_16_mb_bufwriter(b: &mut Bencher) {
        test(b, |f| BufWriter::new(f));
    }

    #[bench]
    fn write_16_mb_no_buf(b: &mut Bencher) {
        test(b, |f| f);
    }
}

mod sequential_write_plus_read {
    use prelude::*;
    use FILE_SIZE;

    #[bench]
    fn write_and_read_16_mb_buf_file(b: &mut Bencher) {
        let mut big_buffer = vec![0u8; FILE_SIZE];
        let kb = vec![0u8; 1024];
        b.iter(|| {
            let mut test_buffile = BufFile::new(tempfile().unwrap()).unwrap();
            for _ in 0..FILE_SIZE/ 1024 {
                test_buffile.write(&kb).unwrap();
            }
            test_buffile.seek(SeekFrom::Start(0)).unwrap();
            for i in 0..FILE_SIZE/ 1024 {
                test_buffile.read(&mut big_buffer[i * 1024 .. (i + 1) * 1024]).unwrap();
            }
        });
        b.bytes = FILE_SIZE as _;
    }

    #[bench]
    fn write_and_read_16_mb_bufwrite_bufread(b: &mut Bencher) {
        let mut big_buffer = vec![0u8; FILE_SIZE];
        let kb = vec![0u8; 1024];
        b.iter(|| {
            let mut test_buffile = BufWriter::new(tempfile().unwrap());
            for _ in 0..FILE_SIZE/ 1024 {
                test_buffile.write(&kb).unwrap();
            }
            let mut file = test_buffile.into_inner().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut test_bufread = BufReader::new(file);
            for i in 0..FILE_SIZE/ 1024 {
                test_bufread.read(&mut big_buffer[i * 1024 .. (i + 1) * 1024]).unwrap();
            }
        });
        b.bytes = FILE_SIZE as _;
    }
}

fn rng() -> XorShiftRng {
    XorShiftRng::from_seed([0, 1, 377, 6712])
}

mod random_full_read_small {
    use prelude::*;
    use rand;
    use rng;
    use seek_bufread::BufReader as SeekBufReader;

    const ITERATIONS: usize = 1024 * 8;

    fn test<R: Read + Seek, F: FnMut(File) -> R>(b: &mut Bencher, mut f: F) {
        let mut buf = vec![0u8; 16];
        let mut rng = rng();
        let distribution = rand::distributions::Range::new(0, (ITERATIONS * 100 - buf.len()) as u64);
        let mut next_idx = || {
            distribution.ind_sample(&mut rng)
        };
        b.iter(|| {
            let file = tempfile().unwrap();
            file.set_len((ITERATIONS * 100) as u64).unwrap();
            let mut test_buffile = f(file);
            for _ in 0..ITERATIONS {
                test_buffile.seek(SeekFrom::Start(next_idx())).unwrap();
                test_buffile.read_exact(&mut buf).unwrap();
            }
        });
        b.bytes = buf.len() as u64 * ITERATIONS as u64;
    }

    #[bench]
    fn write_read_random_buf_file(b: &mut Bencher) {
        test(b, |f| BufFile::new(f).unwrap());
    }

    #[bench]
    fn write_read_random_seek_bufread(b: &mut Bencher) {
        test(b, |f| SeekBufReader::new(f));
    }

    #[bench]
    fn write_read_random_no_buf(b: &mut Bencher) {
        test(b, |f| f);
    }
}

mod weighted_random_read_small {
    use prelude::*;
    use rand;
    use rng;
    use seek_bufread::BufReader as SeekBufReader;
    use FILE_SIZE;

    const ITERATIONS: usize = 1024 * 16;

    fn test<R: Read + Seek, F: FnMut(File) -> R>(b: &mut Bencher, mut f: F) {
        let mut buf = vec![0u8; 16];
        let mut rng = rng();
        let distribution = rand::distributions::Normal::new(1024.0 * 1024.0, 512.0);
        let buf_len = buf.len();
        let mut next_idx = || {
            let x = distribution.ind_sample(&mut rng);
            if x <= 0.0 {
                0
            } else if x >= (FILE_SIZE - buf_len) as f64 {
                (FILE_SIZE - buf_len) as u64
            } else {
                x as u64
            }
        };
        b.iter(|| {
            let file = tempfile().unwrap();
            file.set_len(FILE_SIZE as u64).unwrap();;
            let mut test_buffile = f(file);
            for _ in 0..ITERATIONS {
                test_buffile.seek(SeekFrom::Start(next_idx())).unwrap();
                test_buffile.read_exact(&mut buf).unwrap();
            }
        });
        b.bytes = buf.len() as u64 * ITERATIONS as u64;
    }

    #[bench]
    fn write_read_random_buf_file(b: &mut Bencher) {
        test(b, |f| BufFile::new(f).unwrap());
    }

    #[bench]
    fn write_read_random_seek_bufread(b: &mut Bencher) {
        test(b, |f| SeekBufReader::new(f));
    }

    #[bench]
    fn write_read_random_no_buf(b: &mut Bencher) {
        test(b, |f| f);
    }
}


