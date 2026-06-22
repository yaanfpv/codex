//! Proves the `monitor` tool: a background command's stdout wakes the agent and
//! reaches the model as a labelled user notification (openai/codex#20312).

use std::time::Duration;
use std::time::Instant;

use codex_features::Feature;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::skip_if_sandbox;
use core_test_support::test_codex::test_codex;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_output_wakes_agent() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    let server = start_mock_server().await;

    // Stays alive past the spawn's yield window, then prints one line the
    // delivery consumer must surface, then exits shortly after so the test
    // leaves no long-lived process behind.
    let args = json!({
        "action": "start",
        "command": "sleep 0.5; echo MONITOR_HELLO; sleep 2",
        "description": "signal watch",
    })
    .to_string();

    let mock = mount_sse_sequence(
        &server,
        vec![
            // User turn: the model calls the monitor tool.
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call("call-1", "monitor", &args),
                ev_completed("resp-1"),
            ]),
            // Continuation after the tool result.
            sse(vec![
                ev_assistant_message("msg-1", "watching"),
                ev_completed("resp-2"),
            ]),
            // The turn the monitor's output wakes.
            sse(vec![
                ev_assistant_message("msg-2", "saw it"),
                ev_completed("resp-3"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Monitor)
            .expect("enable monitor feature");
    });
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "watch for the signal".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    // The monitor's stdout line must wake the agent and reach the model as a
    // user-role message, prefixed with the description and carrying only the
    // line (never the process internals).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let delivered = mock.requests().iter().any(|request| {
            request
                .message_input_texts("user")
                .iter()
                .any(|text| text.contains("[signal watch]") && text.contains("MONITOR_HELLO"))
        });
        if delivered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "monitor output never reached the model as a labelled notification"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
