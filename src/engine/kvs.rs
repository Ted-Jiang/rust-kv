extern crate failure;
extern crate log;
extern crate serde;
extern crate serde_derive;

use failure::{format_err, Error};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use serde_derive::{Deserialize, Serialize};

use crate::{KvsError, Result, KvsEngine};
use serde_json::Deserializer;
use std::ffi::OsStr;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use self::failure::_core::sync::atomic::AtomicU64;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use crossbeam_skiplist::SkipMap;
use log::error;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct KvStore {
    // directory for the log and other data
    path: Arc<PathBuf>,
    // map generation number to the file reader
    index: Arc<SkipMap<String, CommandPos>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {

    // create a DB at directory
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        //let mut readers = HashMap::new();
        let index = Arc::new(SkipMap::new());
        //change to mutilthreads
        let mut readers = BTreeMap::new();

        let id_list = sort_id_list(&path)?;

        let mut uncompacted_size = 0;

        for &id in &id_list {
            //open a file reader
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, id))?)?;
            //construct index
            uncompacted_size += load(id, &mut reader, &*index)?;
            readers.insert(id, reader);
        }

        let current_id = id_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_id)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            safe_point,
            //if readers not imple copy use RefCell
            readers: RefCell::new(readers),
        };

        let writer = KvStoreWriter {
            //use for compact
            reader: reader.clone(),
            writer,
            current_id,
            uncompacted_size,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            reader,
            index,
            // only one writer can write to a file
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Create a new log file with given id number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, id: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, id)
    }

}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

     fn get(&self, key: String) -> Result<Option<String>> {
         if let Some(cmd_pos) = self.index.get(&key) {
             if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                 Ok(Some(value))
             } else {
                 Err(KvsError::UnexpectedCommandType)
             }
         } else {
             Ok(None)
         }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &SkipMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut command_iter = Deserializer::from_reader(reader).into_iter::<Command>();
    // number of bytes that can be saved after a compaction.
    // means the size can reuduce!!!
    let mut uncompacted_size = 0;
    while let Some(cmd) = command_iter.next() {
        let new_pos = command_iter.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) =  index.get(&key) {
                    uncompacted_size += old_cmd.value().len;
                }
                // Do Not forget change index!!!
                index.insert(key, (id, pos..new_pos).into());
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted_size += old_cmd.value().len;
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
    //readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, id);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
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

#[derive(Copy, Clone)]
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

/// A single thread reader.
///
/// Each `KvStore` instance has its own `KvStoreReader` and
/// `KvStoreReader`s open the same files separately. So the user
/// can read concurrently through multiple `KvStore`s in different
/// threads.
struct KvStoreReader {
    path: Arc<PathBuf>,
    //id of latest compact file
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl KvStoreReader {
    /// Close file handles with generation number less than safe_point.
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_file_handle(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let x = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= x {
                break;
            }
            readers.remove(&x);
        }
    }

    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R> 
    where 
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>, {
        self.close_file_handle();
        let mut readers = self.readers.borrow_mut();

        if !readers.contains_key(&cmd_pos.id) {
            let reader = BufReaderWithPos::new(File::open(log_path(&self.path, cmd_pos.id))?)?;
            readers.insert(cmd_pos.id, reader);
        }
        let reader = readers.get_mut(&cmd_pos.id).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }

    // Read the log file at the given `CommandPos` and deserialize it to `Command`.
    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }

    /// Close file handles with generation number less than safe_point.
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_gen = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= first_gen {
                break;
            }
            readers.remove(&first_gen);
        }
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> KvStoreReader {
        KvStoreReader {
            //use Arc::clone to  increase the strong reference count.
            path: Arc::clone(&self.path),
            safe_point: Arc::clone(&self.safe_point),
            // don't use other KvStoreReader's readers
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithPos<File>,
    current_id: u64,

    uncompacted_size: u64,
    path:Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandPos>>,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set {key, ..} = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                self.uncompacted_size += old_cmd.value().len;
            }
            self.index.insert(key, (self.current_id, pos..self.writer.pos).into());
        }

        if self.uncompacted_size > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted_size += old_cmd.value().len;
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                self.uncompacted_size += self.writer.pos - pos;
            }

            if self.uncompacted_size > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file
        let compaction_id = self.current_id + 1;
        self.current_id += 2;
        self.writer = new_log_file(&self.path, self.current_id)?;

        let mut compaction_writer = new_log_file(&self.path, compaction_id)?;

        let mut new_pos = 0; // pos in the new log file
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compaction_writer)?)
            })?;
            self.index.insert(
                entry.key().clone(),
                (compaction_id, new_pos..new_pos + len).into(),
            );
            new_pos += len;
        }
        compaction_writer.flush()?;

        self.reader
            .safe_point
            .store(compaction_id, Ordering::SeqCst);
        self.reader.close_stale_handles();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `KvStoreReader`s
        // still keep open file handles. When `KvStoreReader` is used next time, it will clear
        // its stale file handles. On Unix, the files will be deleted after all the handles
        // are closed. On Windows, the deletions below will fail and stale files are expected
        // to be deleted in the next compaction.

        let stale_gens = sort_id_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compaction_id);
        for stale_gen in stale_gens {
            let file_path = log_path(&self.path, stale_gen);
            if let Err(e) = fs::remove_file(&file_path) {
                error!("{:?} cannot be deleted: {}", file_path, e);
            }
        }
        self.uncompacted_size = 0;

        Ok(())
    }
}

