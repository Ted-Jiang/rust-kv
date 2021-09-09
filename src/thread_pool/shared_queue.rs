
use crossbeam::channel::{self, Receiver, Sender};
use crate::thread_pool::ThreadPool;
use std::thread;

use log::{debug, error};

/// If a spawned task panics, the old thread will be destroyed and a new one will be
/// created. It fails silently when any failure to create the thread at the OS level
/// is captured after the thread pool is created. So, the thread number in the pool
/// can decrease to zero, then spawning a task to the thread pool will panic.
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self> where Self: Sized {
        let (tx, rx) = channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();

        //create fix thread to run job
        for _ in 0..threads {
            let rx = TaskReceiver(rx.clone());
            thread::Builder::new().spawn(move || run_tasks(rx))?;
        }
        // only reuturn a producer!
        Ok(SharedQueueThreadPool { tx })
    }

    /// Spawns a function into the thread pool.
    ///
    /// # Panics
    ///
    /// Panics if the thread pool has no thread.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.tx.send(Box::new(job)).expect("The thread pool has no thread.");
    }
}

#[derive(Clone)]
// why use tuple struct
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

// self define Drop for create a new thread
impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_tasks(rx)) {
                error!("Failed to spawn a thread: {}", e);
            }
        }
    }
}

fn run_tasks(rx: TaskReceiver) {
    loop {
        //No Job will block
        match rx.0.recv() {
            Ok(task) => {
                task();
            }
            Err(_) => debug!("Thread exits because the thread pool is destroyed."),
        }
    }
}
