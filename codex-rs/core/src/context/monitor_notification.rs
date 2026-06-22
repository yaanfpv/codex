use super::ContextualUserFragment;

/// A batch of output lines from a `monitor` watcher, delivered as a contextual
/// user fragment so it stays distinguishable from real user input.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MonitorNotification {
    pub(crate) description: String,
    pub(crate) body: String,
}

impl MonitorNotification {
    pub(crate) fn new(description: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            body: body.into(),
        }
    }
}

impl ContextualUserFragment for MonitorNotification {
    fn role(&self) -> &'static str {
        "user"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<monitor_notification>", "</monitor_notification>")
    }

    fn body(&self) -> String {
        format!("\n[{}] {}\n", self.description, self.body)
    }
}
