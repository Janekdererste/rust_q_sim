use crate::simulation::io::worker::Worker;
use crate::simulation::messaging::events::EventsWriter;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;

pub enum Msg {
    Line(Vec<u8>),
    Shutdown,
}

pub struct WorkerGuard {
    _guard: Option<JoinHandle<()>>,
    sender: Sender<Msg>,
    shutdown: Sender<()>,
}

#[derive(Clone, Debug)]
pub struct NonBlocking {
    channel: Sender<Msg>,
}

impl NonBlocking {
    pub fn new<T>(writer: T) -> (NonBlocking, WorkerGuard)
    where
        T: Write + Send + Sync + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        let (shutdown_sender, shutdown_receiver) = mpsc::channel();

        let worker = Worker::new(receiver, writer, shutdown_receiver);
        let guard = WorkerGuard::new(worker.worker_thread(), sender.clone(), shutdown_sender);

        let result = Self { channel: sender };

        (result, guard)
    }

    pub fn from_file(file_path: &str) -> (NonBlocking, WorkerGuard) {
        let file = File::create(file_path).unwrap();
        let writer = BufWriter::new(file);
        NonBlocking::new(writer)
    }
}

impl EventsWriter for NonBlocking {
    fn write(&self, buf: Vec<u8>) {
        self.channel.send(Msg::Line(buf)).unwrap();
    }
}

impl WorkerGuard {
    fn new(handle: JoinHandle<()>, sender: Sender<Msg>, shutdown: Sender<()>) -> Self {
        WorkerGuard {
            _guard: Some(handle),
            sender,
            shutdown,
        }
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        match self.sender.send(Msg::Shutdown) {
            Ok(_) => {
                self.shutdown
                    .send(())
                    .expect("Error when sending shut down message");
            }
            Err(e) => println!(
                "Failed to send shutdown signal to logging worker. Error: {:?}",
                e
            ),
        }
    }
}
