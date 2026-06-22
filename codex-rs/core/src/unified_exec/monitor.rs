//! Command-monitor registry and output delivery.
//!
//! A monitor runs a shell command as a long-lived background process and
//! delivers each stdout line to the session as a notification, waking an idle
//! session at the next turn boundary. It lives inside `unified_exec` so the
//! delivery loop can read the process's `pub(super)` output stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio_util::task::AbortOnDropHandle;

use super::UnifiedExecProcessManager;
use super::process::UnifiedExecProcess;
use crate::session::session::Session;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;

/// Stdout lines emitted within this window coalesce into one notification.
const BATCH_WINDOW: Duration = Duration::from_millis(200);

/// A monitor that emits more than this many lines is auto-stopped so a runaway
/// command cannot wake the agent without bound.
const FLOOD_MAX_LINES: usize = 5000;

/// A snapshot of one active monitor, returned by [`MonitorManager::list`].
pub(crate) struct MonitorInfo {
    pub id: String,
    pub description: String,
    pub command: String,
}

struct MonitorEntry {
    description: String,
    command: String,
    process_id: i32,
    _task: AbortOnDropHandle<()>,
}

/// Per-session registry of active monitors. Holds the delivery tasks; the
/// underlying processes live in the shared [`UnifiedExecProcessManager`] store
/// and are reaped by its `terminate_all_processes` at session shutdown.
#[derive(Default)]
pub(crate) struct MonitorManager {
    monitors: Mutex<HashMap<String, MonitorEntry>>,
}

impl MonitorManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn insert(
        &self,
        id: String,
        process_id: i32,
        description: String,
        command: String,
        task: JoinHandle<()>,
    ) {
        self.monitors.lock().await.insert(
            id,
            MonitorEntry {
                description,
                command,
                process_id,
                _task: AbortOnDropHandle::new(task),
            },
        );
    }

    /// Removes a monitor, returning its process id so the caller can terminate
    /// the underlying process. Dropping the entry aborts its delivery task.
    pub(crate) async fn remove(&self, id: &str) -> Option<i32> {
        self.monitors
            .lock()
            .await
            .remove(id)
            .map(|entry| entry.process_id)
    }

    pub(crate) async fn list(&self) -> Vec<MonitorInfo> {
        self.monitors
            .lock()
            .await
            .iter()
            .map(|(id, entry)| MonitorInfo {
                id: id.clone(),
                description: entry.description.clone(),
                command: entry.command.clone(),
            })
            .collect()
    }

    /// Aborts every monitor's delivery task. The processes themselves are reaped
    /// separately by the unified-exec manager at shutdown.
    pub(crate) async fn abort_all(&self) {
        self.monitors.lock().await.clear();
    }
}

/// Spawns the delivery task for an already-running process, returning its
/// handle. Returns `None` if the process is no longer alive (it exited within
/// the spawn's yield window, so it was never a long-lived watcher).
pub(crate) async fn spawn_delivery(
    manager: &UnifiedExecProcessManager,
    process_id: i32,
    session: Weak<Session>,
    description: String,
) -> Option<JoinHandle<()>> {
    let process = manager.process_by_id(process_id).await?;
    Some(tokio::spawn(delivery_loop(
        process,
        process_id,
        session,
        description,
    )))
}

async fn delivery_loop(
    process: Arc<UnifiedExecProcess>,
    process_id: i32,
    session: Weak<Session>,
    description: String,
) {
    let mut rx = process.output_receiver();
    let exit = process.cancellation_token();
    let mut buf: Vec<u8> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut total_lines: usize = 0;
    let mut flush_at: Option<Instant> = None;

    loop {
        tokio::select! {
            received = rx.recv() => match received {
                Ok(chunk) => {
                    extend_lines(&mut buf, &chunk, &mut pending, &mut total_lines, &mut flush_at);
                    if total_lines > FLOOD_MAX_LINES {
                        flush(&session, &description, &mut pending).await;
                        deliver(
                            &session,
                            &description,
                            format!(
                                "auto-stopped after {FLOOD_MAX_LINES} lines (flood guard); \
                                 restart with a tighter filter"
                            ),
                        )
                        .await;
                        if let Some(session) = session.upgrade() {
                            session
                                .services
                                .unified_exec_manager
                                .terminate_process(process_id)
                                .await;
                        }
                        return;
                    }
                }
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break,
            },
            () = wait_until(flush_at) => {
                flush(&session, &description, &mut pending).await;
                flush_at = None;
            }
            () = exit.cancelled() => break,
        }
    }

    // Drain whatever the broadcast still buffers, then deliver the final lines
    // and an exit notice so the agent learns the watch ended.
    while let Ok(chunk) = rx.try_recv() {
        extend_lines(
            &mut buf,
            &chunk,
            &mut pending,
            &mut total_lines,
            &mut flush_at,
        );
    }
    if !buf.is_empty() {
        let text = String::from_utf8_lossy(&buf);
        let text = text.trim_end();
        if !text.is_empty() {
            pending.push(text.to_string());
        }
    }
    flush(&session, &description, &mut pending).await;
    deliver(&session, &description, exit_notice(&process)).await;
}

/// Resolves at `deadline` when set, otherwise never. The idle arm of the loop.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

fn extend_lines(
    buf: &mut Vec<u8>,
    chunk: &[u8],
    pending: &mut Vec<String>,
    total_lines: &mut usize,
    flush_at: &mut Option<Instant>,
) {
    buf.extend_from_slice(chunk);
    while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
        let line: Vec<u8> = buf.drain(..=nl).collect();
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end();
        if text.is_empty() {
            continue;
        }
        pending.push(text.to_string());
        *total_lines += 1;
        if flush_at.is_none() {
            *flush_at = Some(Instant::now() + BATCH_WINDOW);
        }
    }
}

fn exit_notice(process: &UnifiedExecProcess) -> String {
    if let Some(message) = process.failure_message() {
        format!("watcher ended: {message}")
    } else {
        match process.exit_code() {
            Some(code) => format!("watcher exited (code {code})"),
            None => "watcher exited".to_string(),
        }
    }
}

/// Delivers the accumulated batch as one notification and clears it.
async fn flush(session: &Weak<Session>, description: &str, pending: &mut Vec<String>) {
    if pending.is_empty() {
        return;
    }
    let text = std::mem::take(pending).join("\n");
    deliver(session, description, text).await;
}

/// Wakes the session with one notification, prefixed by the monitor's label.
/// Injects into a running turn, or starts a turn when idle. Lines are dropped
/// only if the session is gone or refuses an idle turn (e.g. Plan mode).
async fn deliver(session: &Weak<Session>, description: &str, body: String) {
    let Some(session) = session.upgrade() else {
        return;
    };
    let text = format!("[{description}] {body}");
    let items = vec![ResponseItem::Message {
        id: None,
        role: "user".to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }];
    if let Err(items) = session.inject_if_running(items).await {
        let _ = session.try_start_turn_if_idle(items).await;
    }
}
