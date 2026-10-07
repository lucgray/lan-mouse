use async_trait::async_trait;
use core::task::{Context, Poll};
use event_thread::EventThread;
use futures::Stream;
use input_event::scancode;
use std::collections::HashMap;
use std::pin::Pin;

use crate::hook_queue::{HookReceiver, channel};

use super::{Capture, CaptureError, CaptureEvent, Position};

mod display_util;
mod event_thread;

pub struct WindowsInputCapture {
    event_rx: HookReceiver,
    event_thread: EventThread,
}

#[async_trait]
impl Capture for WindowsInputCapture {
    fn pending_failure(&self) -> bool {
        self.event_rx.failed()
    }

    async fn create(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.event_thread.create(pos);
        Ok(())
    }

    async fn destroy(&mut self, pos: Position) -> Result<(), CaptureError> {
        self.event_thread.destroy(pos);
        Ok(())
    }

    async fn set_enter_only(&mut self, _pos: Position, _enabled: bool) -> Result<(), CaptureError> {
        Ok(())
    }

    async fn release(&mut self) -> Result<(), CaptureError> {
        self.event_thread.release_capture();
        Ok(())
    }

    async fn release_to(&mut self, t: f64) -> Result<(), CaptureError> {
        self.event_thread.release_capture_to(t);
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), CaptureError> {
        self.event_thread.release_capture();
        Ok(())
    }

    fn set_enter_binds(&mut self, binds: HashMap<Position, Vec<scancode::Linux>>) {
        self.event_thread.set_enter_binds(binds);
    }
}

impl WindowsInputCapture {
    pub(crate) fn new() -> Self {
        let (event_tx, event_rx) = channel();
        let event_thread = EventThread::new(event_tx);
        Self {
            event_thread,
            event_rx,
        }
    }
}

impl Stream for WindowsInputCapture {
    type Item = Result<(Position, CaptureEvent), CaptureError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.event_rx.poll_recv(cx)
    }
}
