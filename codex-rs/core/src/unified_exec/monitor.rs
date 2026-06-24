//! Command-monitor registry and output delivery.
//!
//! A monitor runs a shell command as a long-lived background process and
//! delivers each output line (stdout or stderr) to the session as a
//! notification, waking an idle session at the next turn boundary. It lives
//! inside `unified_exec` so the delivery loop can read the process's
//! `pub(super)` output stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio_util::task::AbortOnDropHandle;

use super::UnifiedExecProcessManager;
use super::async_watcher::TRAILING_OUTPUT_GRACE;
use super::process::UnifiedExecProcess;
use crate::context::ContextualUserFragment;
use crate::context::MonitorNotification;
use crate::session::session::Session;
use codex_protocol::models::ResponseItem;

/// Lines emitted within this window coalesce into one notification.
const BATCH_WINDOW: Duration = Duration::from_millis(200);

/// A monitor that emits more than this many lines is auto-stopped so a runaway
/// command cannot wake the agent without bound.
const FLOOD_MAX_LINES: usize = 5000;

/// A run of bytes with no terminating newline is truncated at this length, so a
/// watcher that streams without newlines (a binary blob, `cat /dev/urandom`)
/// cannot grow the delivery buffer, or one notification, without bound. Each
/// truncation still counts toward the line ceiling, so an endless newline-free
/// stream trips the flood guard instead of exhausting host memory.
const MAX_LINE_BYTES: usize = 16 * 1024;

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

    /// Removes a monitor entry on behalf of its OWN delivery task as that task
    /// exits. `remove` aborts the entry's task by dropping its abort-on-drop
    /// handle, which is correct for an external `action=stop` but would cancel
    /// this task's own final exit-notice delivery; defusing the handle lets the
    /// loop prune itself first and still announce the exit.
    pub(crate) async fn deregister_self(&self, id: &str) {
        if let Some(entry) = self.monitors.lock().await.remove(id) {
            let MonitorEntry { _task, .. } = entry;
            std::mem::forget(_task);
        }
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
/// the spawn's yield window, so it was never a long-lived watcher). `seed` is
/// the output the spawn's initial yield captured before this task subscribed;
/// it is delivered first because the broadcast does not replay to a late
/// subscriber, so without it those early lines would be lost.
pub(crate) async fn spawn_delivery(
    manager: &UnifiedExecProcessManager,
    process_id: i32,
    id: String,
    session: Weak<Session>,
    description: String,
    seed: Vec<u8>,
) -> Option<JoinHandle<()>> {
    let process = manager.process_by_id(process_id).await?;
    Some(tokio::spawn(delivery_loop(
        process,
        process_id,
        id,
        session,
        description,
        seed,
    )))
}

async fn delivery_loop(
    process: Arc<UnifiedExecProcess>,
    process_id: i32,
    id: String,
    session: Weak<Session>,
    description: String,
    seed: Vec<u8>,
) {
    let mut rx = process.output_receiver();
    let exit = process.cancellation_token();
    let mut buf: Vec<u8> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    // Counts lines delivered plus dropped-chunk markers; bounds the total volume
    // a single watcher can wake the agent with. A `Lagged` burst counts by the
    // number of chunks it skipped so a runaway watcher trips the ceiling fast.
    let mut flood_count: usize = 0;
    let mut flush_at: Option<Instant> = None;
    // Set once the process exits: keep draining for a short grace so a final
    // chunk racing the cancellation token still lands (mirrors async_watcher).
    let mut closing_at: Option<Instant> = None;

    // Deliver the initial-yield output the broadcast never replayed to us.
    if extend_lines(
        &mut buf,
        &seed,
        &mut pending,
        &mut flood_count,
        &mut flush_at,
    ) {
        stop_for_flood(&session, &description, &id, process_id, &mut pending).await;
        return;
    }

    loop {
        tokio::select! {
            received = rx.recv() => {
                let flooded = match received {
                    Ok(chunk) => {
                        extend_lines(&mut buf, &chunk, &mut pending, &mut flood_count, &mut flush_at)
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        // The watcher outran the output channel. Surface the gap
                        // instead of dropping it silently, and count it toward the
                        // flood guard so a runaway command still trips the ceiling.
                        flood_count += skipped as usize;
                        pending.push(format!(
                            "(dropped ~{skipped} chunks: watcher output too fast)"
                        ));
                        if flush_at.is_none() {
                            flush_at = Some(Instant::now() + BATCH_WINDOW);
                        }
                        flood_count >= FLOOD_MAX_LINES
                    }
                    Err(RecvError::Closed) => break,
                };
                if flooded {
                    stop_for_flood(&session, &description, &id, process_id, &mut pending).await;
                    return;
                }
            }
            () = wait_until(flush_at) => {
                flush(&session, &description, &mut pending).await;
                flush_at = None;
            }
            () = exit.cancelled(), if closing_at.is_none() => {
                closing_at = Some(Instant::now() + TRAILING_OUTPUT_GRACE);
            }
            () = wait_until(closing_at) => break,
        }
    }

    // Drain whatever the broadcast still buffers, then deliver the final lines
    // and an exit notice so the agent learns the watch ended. A Lagged error is
    // surfaced and skipped (matching the in-loop arm) rather than ending the
    // drain early and dropping the chunks still queued behind it.
    loop {
        match rx.try_recv() {
            Ok(chunk) => {
                extend_lines(
                    &mut buf,
                    &chunk,
                    &mut pending,
                    &mut flood_count,
                    &mut flush_at,
                );
            }
            Err(TryRecvError::Lagged(skipped)) => {
                flood_count += skipped as usize;
                pending.push(format!(
                    "(dropped ~{skipped} chunks: watcher output too fast)"
                ));
            }
            Err(_) => break,
        }
    }
    if !buf.is_empty() {
        let text = String::from_utf8_lossy(&buf);
        let text = text.trim_end();
        if !text.is_empty() {
            pending.push(text.to_string());
        }
    }
    flush(&session, &description, &mut pending).await;
    // Prune the registry BEFORE announcing the exit, so a turn the exit notice
    // wakes (e.g. an `action=list`) already sees this watcher gone. A concurrent
    // `action=stop` may have removed the entry first; this is then a no-op.
    if let Some(session) = session.upgrade() {
        session.services.monitor_manager.deregister_self(&id).await;
    }
    deliver(&session, &description, exit_notice(&process)).await;
}

/// Auto-stops a watcher that hit the flood ceiling: flush what we have, tell the
/// agent, terminate the process, and prune the registry. Used by both the seed
/// and the receive paths.
async fn stop_for_flood(
    session: &Weak<Session>,
    description: &str,
    id: &str,
    process_id: i32,
    pending: &mut Vec<String>,
) {
    flush(session, description, pending).await;
    // Terminate and deregister before announcing, so the agent wakes to a
    // consistent state.
    if let Some(session) = session.upgrade() {
        session
            .services
            .unified_exec_manager
            .terminate_process(process_id)
            .await;
        session.services.monitor_manager.deregister_self(id).await;
    }
    deliver(
        session,
        description,
        format!(
            "auto-stopped after {FLOOD_MAX_LINES} lines (flood guard); \
             restart with a tighter filter"
        ),
    )
    .await;
}

/// Resolves at `deadline` when set, otherwise never.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Splits `chunk` into complete lines, appending each to `pending` and arming
/// the batch timer. Returns `true` if the flood ceiling was reached mid-chunk,
/// so the caller can auto-stop before a single huge chunk blows past the bound.
fn extend_lines(
    buf: &mut Vec<u8>,
    chunk: &[u8],
    pending: &mut Vec<String>,
    flood_count: &mut usize,
    flush_at: &mut Option<Instant>,
) -> bool {
    buf.extend_from_slice(chunk);
    loop {
        let line: Vec<u8> = if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            buf.drain(..=nl).collect()
        } else if buf.len() > MAX_LINE_BYTES {
            // No newline yet, but the buffer is already pathologically long.
            // Emit a truncated prefix so a newline-free stream cannot grow the
            // buffer without bound; the remainder keeps draining on later passes,
            // and each truncation counts toward the flood ceiling below.
            let mut line: Vec<u8> = buf.drain(..MAX_LINE_BYTES).collect();
            line.extend_from_slice(b" ... (line truncated)");
            line
        } else {
            break;
        };
        let text = String::from_utf8_lossy(&line);
        let text = text.trim_end();
        if text.is_empty() {
            continue;
        }
        pending.push(text.to_string());
        *flood_count += 1;
        if flush_at.is_none() {
            *flush_at = Some(Instant::now() + BATCH_WINDOW);
        }
        if *flood_count >= FLOOD_MAX_LINES {
            return true;
        }
    }
    false
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
/// Injects into a running turn, or starts one when idle. The output is never
/// silently dropped: if the idle start loses a race (a turn began between the
/// two checks) or is refused (Plan mode), the items are recorded against the
/// turn instead. That matters most for the final "watcher exited" notice, which
/// fires after the loop has already deregistered itself and so has no retry.
async fn deliver(session: &Weak<Session>, description: &str, body: String) {
    let Some(session) = session.upgrade() else {
        return;
    };
    let items: Vec<ResponseItem> = vec![ContextualUserFragment::into(MonitorNotification::new(
        description,
        body,
    ))];
    if let Err(items) = session.inject_if_running(items).await
        && let Err(err) = session.try_start_turn_if_idle(items).await
    {
        session.inject_no_new_turn(err.into_input(), None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_tracks_insert_list_and_remove() {
        let manager = MonitorManager::new();
        manager
            .insert(
                "mon_a".to_string(),
                1,
                "watch a".to_string(),
                "cmd a".to_string(),
                tokio::spawn(async {}),
            )
            .await;
        manager
            .insert(
                "mon_b".to_string(),
                2,
                "watch b".to_string(),
                "cmd b".to_string(),
                tokio::spawn(async {}),
            )
            .await;

        assert_eq!(manager.list().await.len(), 2);

        // `remove` returns the process id so the caller can terminate it; a
        // second remove of the same id is a no-op.
        assert_eq!(manager.remove("mon_a").await, Some(1));
        assert_eq!(manager.remove("mon_a").await, None);

        let remaining = manager.list().await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, "mon_b");

        manager.abort_all().await;
        assert!(manager.list().await.is_empty());
    }

    #[tokio::test]
    async fn deregister_self_removes_entry_without_aborting_its_task() {
        let manager = MonitorManager::new();
        // A task that runs until aborted, so we can observe whether it survives.
        let task = tokio::spawn(std::future::pending::<()>());
        let handle = task.abort_handle();
        manager
            .insert(
                "mon_x".to_string(),
                7,
                "watch".to_string(),
                "cmd".to_string(),
                task,
            )
            .await;
        assert_eq!(manager.list().await.len(), 1);

        manager.deregister_self("mon_x").await;
        assert!(manager.list().await.is_empty(), "entry pruned");
        // Unlike `remove`, deregister_self must NOT abort the entry's task: the
        // loop removing itself still has its final exit notice to deliver.
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "deregister_self must not abort the entry's task"
        );

        // Deregistering an absent id is a no-op.
        manager.deregister_self("mon_x").await;
        assert!(manager.list().await.is_empty());

        handle.abort();
    }
}
