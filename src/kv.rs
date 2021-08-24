extern crate failure;
extern crate log;
extern crate serde;
extern crate serde_derive;

use failure::{format_err, Error};
use std::collections::{HashMap, BTreeMap};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use serde::Deserialize as SerdeDe;
use serde_derive::{Deserialize, Serialize};

use crate::{KvsError, Result};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write, BufWriter};
use std::ops::Range;
use std::ffi::OsStr;
use serde_json::Deserializer;

const FILE_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStore {
    // dictionary for data and log files
    path: PathBuf,
    //map file id to reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    //log writer
    writer:BufWriterWithPos<File>,
    //current ID
    current_id: u64,
    index: BTreeMap<String, CommandPos>,

    uncompacted_size: u64,

}

impl KvStore {

    pub fn set(&mut self, key: String, value: String) -> Result<()> {

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {

        Ok(None)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {

        Ok(())
    }

    // create a DB at directory
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let id_list = sort_id_list(&path)?;

        let mut uncompacted_size = 0;
        for &id in & id_list {
            //open a file reader
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, id))?)?;
            uncompacted_size += load(id, &mut reader, &mut index)?;
            readers.insert(id, reader);
        }
        let current_id = id_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_id, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_id,
            index,
            uncompacted_size,
        })
    }

}


/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut command_iter = Deserializer::from_reader(reader).into_iter::<Command>();
    // number of bytes that can be saved after a compaction.
    // means the size can reuduce!!!
    let mut uncompacted_size = 0;
    while let Some(cmd) = command_iter.next() {
        let new_pos = command_iter.byte_offset() as u64;
        match cmd? {
            Command::Set {key, ..} => {
                if let Some(old_cmd) = index.insert(key, (id, pos..new_pos).into()) {
                    uncompacted_size += old_cmd.len;
                }
            }
            Command::Remove {key} => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted_size += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted_size += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted_size)
}
// reutrn sorted id number in given directory
fn sort_id_list(p0: &PathBuf) -> Result<Vec<u64>> {
    //
    let mut id_list: Vec<u64> = fs::read_dir(&p0)?
        // like in all file in a directory
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        // filter remain file end with log
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                //use turbofish to parse to u64
                .map(str::parse::<u64>)
        })
        // iterator of iterators to one iterator
        .flatten()
        .collect();
    id_list.sort_unstable();
    Ok(id_list)
}

fn new_log_file(
    path: &Path,
    id: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, id);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    // also need add a reader
    readers.insert(id, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}


/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

struct CommandPos {
    id: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((id, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            // same represent
            id,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}