//! `CreateFlow` command handler: creates a **brand-new flow** with its first version.

use async_trait::async_trait;

use crate::RejectionType;
use crate::command::Command;
use crate::event::Event;
use crate::flow::Flow;
use crate::flow::FlowStatus;
use crate::flow_version::FlowVersion;
use crate::handler::{Collector, CommandHandler, HandlerContext};
use crate::id::{FlowName, FlowVersionId, RequestId};
use crate::log::Timestamp;

/// Handles the creation of a new flow **definition** (a new [`FlowName`] with its first immutable
/// version).
///
/// This is the CCES analogue of Zeebe's Deployment processing: the single place a full definition
/// enters the system for a **new** name. The handler assigns every identity — minting a fresh audit
/// [`FlowId`](crate::id::FlowId), a [`FlowVersionId`] (the identity executions bind to), and the
/// first `version` ordinal (`1`) — then emits both [`Event::FlowCreated`] (the flow aggregate's
/// birth) and [`Event::FlowVersionCreated`] (this first version) in one atomic command batch. From
/// then on, executions reference only a `flow_version_id` and the machine is resolved from Storage
/// by id, never carried in a command again.
///
/// `CreateFlow` is **only** for a new name: the [`Engine::create_flow`](crate::Engine::create_flow)
/// boundary pre-checks (and rejects) an existing name before appending, and the handler re-checks
/// defensively below so a command forged straight into the log can never alias a second creation
/// onto a live flow. Creating an *additional* version of an existing flow is a distinct future
/// operation that emits only [`Event::FlowVersionCreated`] — for which that event is split out.
///
/// The definition travels as its **raw ASL string** (the `definition` field). The boundary
/// validates it before anything is written, but the handler re-parses defensively so a command
/// forged straight into the log can never fold a non-parseable definition into Storage — the
/// invariant `FlowVersion.definition` always parses.
#[derive(Default)]
pub struct CreateFlowHandler;

#[async_trait]
impl CommandHandler for CreateFlowHandler {
    fn command(&self) -> Command {
        Command::CreateFlow {
            request_id: RequestId::nil(),
            name: FlowName::new("default").expect("static placeholder name is valid"),
            definition: String::new(),
        }
    }

    async fn handle(&self, cmd: &Command, ctx: &mut HandlerContext<'_>, out: &mut Collector) {
        let Command::CreateFlow {
            request_id,
            name,
            definition,
        } = cmd
        else {
            unreachable!(
                "command dispatch guarantees the handler receives its own variant; got {cmd:?}"
            );
        };

        // Defensive parse/validation before persisting: at the normal boundary the definition was
        // already validated by `Engine::create_flow`, but a replayed or forged command must not
        // write a definition that `HandlerContext::machine` would later fail to parse. Reads the
        // string, discards the model — the durable record stays the raw string.
        if serde_json::from_str::<spica_asl::StateMachine>(definition).is_err() {
            // Creating a definition has no execution entity to fail, so a malformed definition
            // cannot be turned into an execution failure; instead we **reject** the command — emit a
            // `Reject` record so the awaiting caller (and the log) receive a definitive
            // `INVALID_ARGUMENT` response rather than a silent no-entry handler return. This is the
            // Zeebe model: a well-formed-but-unapplicable command gets a COMMAND_REJECTION, not
            // silence.
            tracing::warn!(name = %name, "create_flow: rejecting malformed definition");
            out.reject(
                *request_id,
                RejectionType::InvalidArgument,
                format!("create_flow: malformed definition for flow {name}"),
            );
            return;
        }

        // `CreateFlow` creates a **new** flow only. The `Engine` boundary rejects an existing name
        // before appending, but the handler re-checks atomically here (it is the serialized point
        // the StreamProcessor reaches in strict log order): a command forged straight into the log must
        // never alias a second creation onto a live flow. The awaiting caller's boundary check may
        // have been racy (two concurrent same-name creates both passed its read), so this is the
        // authoritative point — and the loser must surface `ALREADY_EXISTS` to its awaiter (and the
        // log) via a `Reject`, not a silent drop.
        if let Ok(Some(_)) = ctx.storage.get_flow_by_name(name.clone()).await {
            tracing::warn!(name = %name, "create_flow: rejecting — flow already exists");
            out.reject(
                *request_id,
                RejectionType::AlreadyExists,
                format!("create_flow: flow {name} already exists"),
            );
            return;
        }

        // Assign every durable identity (the handler is the sole identity-assigner): the flow's audit
        // `flow_id` and this version's `flow_version_id` are minted fresh; a new flow's first version
        // is always ordinal 1. `created_at` is stamped now and carried on both rows.
        let flow_id = crate::id::FlowId::new();
        let flow_version_id = FlowVersionId::new();
        let created_at = Timestamp::now();
        tracing::info!(name = %name, %flow_id, %flow_version_id, "flow with first version created");

        // Emit the flow's birth and its first version in one atomic batch (same cause/stream). The
        // StreamProcessor routes the caller's ack on `FlowVersionCreated` (see `Event::FlowVersionCreated`).
        out.emit_event(Event::FlowCreated {
            request_id: *request_id,
            flow: Flow {
                flow_id,
                name: name.clone(),
                created_at,
                // A flow is born at `created_at`; its update time starts there too (later versions
                // advance `updated_at` via the version applier).
                updated_at: created_at,
                status: FlowStatus::Active,
                // A flow is born with its first version; the version applier reconciles this pointer.
                latest_flow_version_id: flow_version_id,
            },
        });
        // The CreateFlow caller awaits the request echoed on `FlowVersionCreated` (this version's
        // birth); declare that ack so the StreamProcessor completes the awaiting caller only once this
        // version event is durably applied to Storage (see `AckSideEffect::CompleteRequest`).
        let version_event = Event::FlowVersionCreated {
            request_id: *request_id,
            flow_version: FlowVersion {
                flow_version_id,
                flow_id,
                name: name.clone(),
                version: 1,
                definition: definition.clone(),
                created_at,
            },
        };
        out.ack_request(*request_id, version_event.clone());
        out.emit_event(version_event);
    }
}
