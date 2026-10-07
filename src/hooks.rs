//! Bounded, ordered execution of user-configured screen transition commands.
use lan_mouse_ipc::ClientHandle;
use std::{cell::RefCell, collections::VecDeque, io, rc::Rc, time::Duration};
use tokio::{process::Command, sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const MAX_PENDING: usize = 64;
const HOOK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub(crate) enum HookKind {
    Enter,
    Leave,
}

impl std::fmt::Display for HookKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Enter => "enter",
            Self::Leave => "leave",
        })
    }
}

struct Request {
    handle: ClientHandle,
    kind: HookKind,
    command: String,
}
type Active = Rc<RefCell<Option<(ClientHandle, CancellationToken)>>>;

pub(crate) struct HookRunner {
    pending: Rc<RefCell<VecDeque<Request>>>,
    active: Active,
    ready: Rc<Notify>,
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl HookRunner {
    pub(crate) fn new() -> Self {
        let pending = Rc::new(RefCell::new(VecDeque::new()));
        let active = Rc::new(RefCell::new(None));
        let ready = Rc::new(Notify::new());
        let cancellation = CancellationToken::new();
        let task = tokio::task::spawn_local(run(
            pending.clone(),
            active.clone(),
            ready.clone(),
            cancellation.clone(),
        ));
        Self {
            pending,
            active,
            ready,
            cancellation,
            task: Some(task),
        }
    }

    /// Returns a notice when overload required skipping queued commands.
    pub(crate) fn submit(
        &self,
        handle: ClientHandle,
        kind: HookKind,
        command: String,
    ) -> Option<String> {
        let notice = enqueue(
            &mut self.pending.borrow_mut(),
            Request {
                handle,
                kind,
                command,
            },
        );
        self.ready.notify_one();
        notice
    }

    pub(crate) fn cancel_client(&self, handle: ClientHandle) {
        self.pending.borrow_mut().retain(|r| r.handle != handle);
        if let Some((current, token)) = self.active.borrow().as_ref() {
            if *current == handle {
                token.cancel();
            }
        }
    }

    pub(crate) async fn terminate(&mut self) {
        self.cancellation.cancel();
        self.pending.borrow_mut().clear();
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(Duration::from_secs(2), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
    }
}

impl Drop for HookRunner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.pending.borrow_mut().clear();
    }
}

fn enqueue(pending: &mut VecDeque<Request>, request: Request) -> Option<String> {
    let notice = if pending.len() == MAX_PENDING {
        let previous = pending.len();
        pending.retain(|r| r.handle != request.handle);
        let removed = previous - pending.len();
        if removed == 0 {
            return Some(format!(
                "Hook queue is full; command for device {} was not started",
                request.handle
            ));
        }
        Some(format!(
            "Hook queue is full; skipped {removed} older commands for device {} and queued its latest command",
            request.handle
        ))
    } else {
        None
    };
    pending.push_back(request);
    notice
}

async fn run(
    pending: Rc<RefCell<VecDeque<Request>>>,
    active: Active,
    ready: Rc<Notify>,
    cancellation: CancellationToken,
) {
    loop {
        if cancellation.is_cancelled() {
            break;
        }
        let request = pending.borrow_mut().pop_front();
        let Some(request) = request else {
            tokio::select! { _ = ready.notified() => {}, _ = cancellation.cancelled() => break }
            continue;
        };
        let token = cancellation.child_token();
        *active.borrow_mut() = Some((request.handle, token.clone()));
        log::info!(
            "starting {} hook for client {}",
            request.kind,
            request.handle
        );
        match execute(&request.command, &token, HOOK_TIMEOUT).await {
            Ok(status) if status.success() => log::info!(
                "{} hook for client {} completed",
                request.kind,
                request.handle
            ),
            Ok(status) => log::warn!(
                "{} hook for client {} exited with {status}",
                request.kind,
                request.handle
            ),
            Err(error) => log::warn!(
                "{} hook for client {} failed: {error}",
                request.kind,
                request.handle
            ),
        }
        active.borrow_mut().take();
    }
}

#[cfg(unix)]
struct ProcessGroup(Option<u32>);
#[cfg(unix)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            // The child remains owned until this guard is dropped; its group ID
            // cannot be reused while it is running.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}

#[cfg(windows)]
struct ProcessJob(windows::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl ProcessJob {
    fn attach(child: &tokio::process::Child) -> io::Result<Self> {
        use windows::{
            Win32::{
                Foundation::HANDLE,
                System::JobObjects::{AssignProcessToJobObject, CreateJobObjectW},
            },
            core::PCWSTR,
        };
        let job =
            Self(unsafe { CreateJobObjectW(None, PCWSTR::null()) }.map_err(io::Error::other)?);
        job.set_kill_on_close(true)?;
        let process = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("hook process handle unavailable"))?;
        unsafe { AssignProcessToJobObject(job.0, HANDLE(process)) }.map_err(io::Error::other)?;
        Ok(job)
    }

    fn set_kill_on_close(&self, enabled: bool) -> io::Result<()> {
        use windows::Win32::System::JobObjects::{
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectExtendedLimitInformation, SetInformationJobObject,
        };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        if enabled {
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        }
        unsafe {
            SetInformationJobObject(
                self.0,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        }
        .map_err(io::Error::other)
    }
}

#[cfg(windows)]
impl Drop for ProcessJob {
    fn drop(&mut self) {
        let _ = unsafe { windows::Win32::Foundation::CloseHandle(self.0) };
    }
}

async fn execute(
    command: &str,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> io::Result<std::process::ExitStatus> {
    if cancellation.is_cancelled() {
        return Err(io::ErrorKind::Interrupted.into());
    }
    #[cfg(unix)]
    let mut process = {
        let mut process = Command::new("sh");
        process.arg("-c").arg(command).process_group(0);
        process
    };
    #[cfg(windows)]
    let mut process = {
        let mut process = Command::new("cmd.exe");
        process.arg("/C").arg(command);
        process.creation_flags(windows::Win32::System::Threading::CREATE_NO_WINDOW.0);
        process
    };
    process.kill_on_drop(true);
    let mut child = process.spawn()?;
    #[cfg(unix)]
    let mut group = ProcessGroup(child.id());
    #[cfg(windows)]
    let job = match ProcessJob::attach(&child) {
        Ok(job) => job,
        Err(error) => {
            // A very short command may finish before job assignment.
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            return Err(error);
        }
    };
    let reason = tokio::select! {
        result = child.wait() => {
            #[cfg(unix)]
            if result.is_ok() { group.0 = None; }
            #[cfg(windows)]
            if result.is_ok() { job.set_kill_on_close(false)?; }
            return result;
        },
        _ = cancellation.cancelled() => io::ErrorKind::Interrupted,
        _ = tokio::time::sleep(timeout) => io::ErrorKind::TimedOut,
    };
    #[cfg(unix)]
    drop(group);
    #[cfg(windows)]
    drop(job);
    child.start_kill()?;
    let _ = child.wait().await;
    Err(io::Error::new(
        reason,
        "hook cancelled or exceeded its time limit",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overload_keeps_latest_device_action_with_bounded_backlog() {
        let mut pending = VecDeque::new();
        for index in 0..100 {
            enqueue(
                &mut pending,
                Request {
                    handle: 1,
                    kind: HookKind::Enter,
                    command: index.to_string(),
                },
            );
            assert!(pending.len() <= MAX_PENDING);
        }
        assert_eq!(pending.back().unwrap().command, "99");
        let commands: Vec<usize> = pending.iter().map(|r| r.command.parse().unwrap()).collect();
        assert!(commands.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(commands.first(), Some(&64));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_windows_shell_runs_and_times_out() {
        let status = execute(
            "exit /b 0",
            &CancellationToken::new(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(status.success());
        let error = execute(
            "ping -n 30 127.0.0.1 > nul",
            &CancellationToken::new(),
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_and_cancellation_stop_commands_before_late_side_effects() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("late");
        let command = format!("sleep 1; printf late > '{}'", output.display());
        let error = execute(
            &command,
            &CancellationToken::new(),
            Duration::from_millis(30),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let token = CancellationToken::new();
        let cancellation = token.clone();
        let task =
            tokio::spawn(async move { execute(&command, &token, Duration::from_secs(30)).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancellation.cancel();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(!output.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn deleting_device_cancels_its_work_and_preserves_other_device() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let directory = tempfile::tempdir().unwrap();
                let output = directory.path().join("remaining");
                let mut runner = HookRunner::new();
                runner.submit(1, HookKind::Enter, "sleep 30".into());
                runner.submit(
                    1,
                    HookKind::Leave,
                    format!("printf deleted >> '{}'", output.display()),
                );
                runner.submit(
                    2,
                    HookKind::Enter,
                    format!("printf other >> '{}'", output.display()),
                );
                tokio::time::timeout(Duration::from_secs(2), async {
                    while runner.active.borrow().is_none() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                runner.cancel_client(1);
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if std::fs::read_to_string(&output).is_ok_and(|s| s == "other") {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                runner.terminate().await;
                assert_eq!(std::fs::read_to_string(output).unwrap(), "other");
            })
            .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runner_preserves_order_and_shutdown_cancels_active_and_pending() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let directory = tempfile::tempdir().unwrap();
                let output = directory.path().join("order");
                let mut runner = HookRunner::new();
                runner.submit(
                    1,
                    HookKind::Enter,
                    format!("sleep 0.03; printf enter >> '{}'", output.display()),
                );
                runner.submit(
                    1,
                    HookKind::Leave,
                    format!("printf leave >> '{}'", output.display()),
                );
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if std::fs::read_to_string(&output).is_ok_and(|s| s == "enterleave") {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                runner.submit(1, HookKind::Enter, "sleep 30".into());
                runner.submit(
                    1,
                    HookKind::Leave,
                    format!("printf unexpected >> '{}'", output.display()),
                );
                tokio::time::sleep(Duration::from_millis(30)).await;
                runner.terminate().await;
                assert!(runner.pending.borrow().is_empty());
                assert!(runner.active.borrow().is_none());
                assert_eq!(std::fs::read_to_string(output).unwrap(), "enterleave");
            })
            .await;
    }
}
