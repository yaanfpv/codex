use std::collections::BTreeMap;
use std::sync::Arc;

use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::resolve_tool_environment;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::spawn_delivery;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde_json::json;

use super::ExecCommandArgs;
use super::get_command;
use super::shell_mode_for_environment;

const MONITOR_TOOL_NAME: &str = "monitor";

/// Time the spawn blocks for initial output before returning. Kept short so the
/// tool call returns quickly while the watcher keeps running in the background.
const MONITOR_YIELD_MS: u64 = 250;

pub struct MonitorHandler;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MonitorAction {
    Start,
    Stop,
    List,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    action: MonitorAction,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    id: Option<String>,
}

fn create_monitor_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "action".to_string(),
            JsonSchema::string_enum(
                vec![json!("start"), json!("stop"), json!("list")],
                Some("Which monitor operation to perform.".to_string()),
            ),
        ),
        (
            "command".to_string(),
            JsonSchema::string(Some(
                "Shell command to run as the watcher (action=start). Each line it prints to stdout becomes one notification, so filter to the lines you care about (e.g. `tail -F app.log | grep --line-buffered ERROR`).".to_string(),
            )),
        ),
        (
            "description".to_string(),
            JsonSchema::string(Some(
                "Short label prefixed to every notification this watcher emits (action=start), e.g. \"errors in app.log\".".to_string(),
            )),
        ),
        (
            "id".to_string(),
            JsonSchema::string(Some(
                "The monitor id returned by a previous start (action=stop).".to_string(),
            )),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: MONITOR_TOOL_NAME.to_string(),
        description: "Run a shell command as a long-lived background watcher. Each line the command prints to stdout is delivered to you as a notification, prefixed with the label; lines emitted close together are batched. The watch ends when the command exits. Use it to react to events without polling: `fswatch <path>` or `inotifywait -m <path>` for file changes, `tail -F <log> | grep --line-buffered <pattern>` for log signals, or a poll loop for remote state. action=start begins a watch and returns its id; action=stop ends the watch with that id; action=list shows active watches."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["action".to_string()]),
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}

impl ToolExecutor<ToolInvocation> for MonitorHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(MONITOR_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_monitor_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(handle_call(invocation))
    }
}

impl CoreToolRuntime for MonitorHandler {}

async fn handle_call(
    invocation: ToolInvocation,
) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        call_id,
        payload,
        ..
    } = invocation;
    let ToolPayload::Function { arguments } = payload else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{MONITOR_TOOL_NAME} handler received unsupported payload"
        )));
    };
    let args: MonitorArgs = parse_arguments(&arguments)?;

    match args.action {
        MonitorAction::Start => {
            start(&session, &turn, call_id, args.command, args.description).await
        }
        MonitorAction::Stop => {
            let Some(id) = args.id else {
                return Err(FunctionCallError::RespondToModel(
                    "action=stop requires `id`".to_string(),
                ));
            };
            let message = match session.services.monitor_manager.remove(&id).await {
                Some(process_id) => {
                    session
                        .services
                        .unified_exec_manager
                        .terminate_process(process_id)
                        .await;
                    format!("Stopped monitor {id}.")
                }
                None => format!("No active monitor with id {id}."),
            };
            Ok(text_output(message))
        }
        MonitorAction::List => {
            let monitors = session.services.monitor_manager.list().await;
            let message = if monitors.is_empty() {
                "No active monitors.".to_string()
            } else {
                monitors
                    .iter()
                    .map(|m| format!("{}  [{}]  {}", m.id, m.description, m.command))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            Ok(text_output(message))
        }
    }
}

async fn start(
    session: &Arc<crate::session::session::Session>,
    turn: &Arc<crate::session::turn_context::TurnContext>,
    call_id: String,
    command: Option<String>,
    description: Option<String>,
) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
    let command = command.filter(|c| !c.trim().is_empty()).ok_or_else(|| {
        FunctionCallError::RespondToModel("action=start requires a non-empty `command`".to_string())
    })?;
    let description = description
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "action=start requires a non-empty `description`".to_string(),
            )
        })?;

    let manager = &session.services.unified_exec_manager;
    let context = UnifiedExecContext::new(session.clone(), turn.clone(), call_id);
    let Some(turn_environment) = resolve_tool_environment(turn.as_ref(), None)? else {
        return Err(FunctionCallError::RespondToModel(
            "unified exec is unavailable in this session".to_string(),
        ));
    };
    let cwd = turn_environment.cwd().clone();
    let environment = Arc::clone(&turn_environment.environment);
    let shell_mode =
        shell_mode_for_environment(&turn.unified_exec_shell_mode, environment.as_ref());
    let shell = turn_environment
        .shell
        .clone()
        .map(Arc::new)
        .unwrap_or_else(|| session.user_shell());

    let exec_args = ExecCommandArgs::for_command(command.clone());
    let resolved = get_command(
        &exec_args,
        shell,
        &shell_mode,
        turn.config.permissions.allow_login_shell,
    )
    .map_err(FunctionCallError::RespondToModel)?;

    let process_id = manager.allocate_process_id().await;
    let request = ExecCommandRequest {
        command: resolved.command,
        shell_type: resolved.shell_type,
        hook_command: command.clone(),
        process_id,
        yield_time_ms: MONITOR_YIELD_MS,
        max_output_tokens: None,
        cwd: cwd.clone(),
        sandbox_cwd: cwd,
        turn_environment: turn_environment.clone(),
        shell_mode,
        network: turn.network.clone(),
        tty: false,
        sandbox_permissions: Default::default(),
        additional_permissions: None,
        additional_permissions_preapproved: false,
        justification: None,
        prefix_rule: None,
    };

    if let Err(err) = manager.exec_command(request, &context).await {
        manager.release_process_id(process_id).await;
        return Err(FunctionCallError::RespondToModel(format!(
            "failed to start monitor: {err:?}"
        )));
    }

    let Some(task) = spawn_delivery(
        manager,
        process_id,
        Arc::downgrade(session),
        description.clone(),
    )
    .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "command exited immediately; a monitor command must keep running and print events"
                .to_string(),
        ));
    };

    let id = format!("mon_{}", uuid::Uuid::new_v4());
    session
        .services
        .monitor_manager
        .insert(id.clone(), process_id, description.clone(), command, task)
        .await;
    Ok(text_output(format!(
        "Started monitor {id}: watching \"{description}\". Stop it with action=stop, id={id}."
    )))
}

fn text_output(message: String) -> Box<dyn crate::tools::context::ToolOutput> {
    boxed_tool_output(FunctionToolOutput::from_text(
        message,
        /*success*/ Some(true),
    ))
}
