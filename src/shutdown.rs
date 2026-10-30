use std::io;
use tokio::signal::unix::{Signal, SignalKind, signal};

/// Register before initialization, retaining a stop request between async phases.
pub struct Shutdown {
    interrupt: Signal,
    terminate: Signal,
    requested: bool,
}

impl Shutdown {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            requested: false,
        })
    }

    pub async fn cancelled(&mut self) -> io::Result<()> {
        if !self.requested {
            tokio::select! {
                _ = self.interrupt.recv() => {},
                _ = self.terminate.recv() => {},
            }
            self.requested = true;
        }
        Ok(())
    }
}

pub async fn cancelled() -> io::Result<()> {
    Shutdown::new()?.cancelled().await
}
