//! The `Flow` element — a workflow **definition**: the name executions address, and the immutable
//! versions that carry its ASL. Only its birth lives here; a version is never mutated afterwards.

use crate::RejectionType;
use crate::handler::{Collector, HandlerContext, ProcessingError};
use crate::types::command::CreateFlow;
use crate::types::event::{Event, FlowCreated, FlowVersionCreated};
use crate::types::flow::Flow;
use crate::types::flow_version::FlowVersion;

pub(crate) struct FlowElementBehavior;

impl FlowElementBehavior {
    /// Create a **new** flow definition — a new name with its first immutable version — and hand back
    /// the two rows it births.
    ///
    /// This is the CCES analogue of Zeebe's Deployment processing: the single place a full definition
    /// enters the system for a **new** name. This element assigns the durable identity — minting a fresh
    /// version reference (its `{flow_name}-{version}` object name plus a never-reused `uid`, the
    /// identity executions bind to, at the first `version` ordinal (`1`)) — then emits both
    /// [`Event::FlowCreated`] (the flow aggregate's birth) and [`Event::FlowVersionCreated`]
    /// (this first version) in one atomic command batch. From then on, executions reference only a
    /// version reference and the machine is resolved from Storage by it, never carried in a command again.
    ///
    /// `CreateFlow` is **only** for a new name: the [`Engine::create_flow`](crate::Engine::create_flow)
    /// boundary pre-checks (and rejects) an existing name before appending, and this re-checks
    /// defensively below so a command forged straight into the log can never alias a second creation
    /// onto a live flow. Creating an *additional* version of an existing flow is a distinct future
    /// operation that emits only [`Event::FlowVersionCreated`] — for which that event is split out.
    ///
    /// The definition travels as its **raw ASL string** (the `definition` field). The boundary
    /// validates it before anything is written, but this re-parses defensively so a command
    /// forged straight into the log can never fold a non-parseable definition into Storage — the
    /// invariant `FlowVersion.definition` always parses.
    pub(crate) async fn create(
        p: &CreateFlow,
        ctx: &mut HandlerContext<'_>,
        out: &mut Collector<'_>,
    ) -> Result<(), ProcessingError> {
        let CreateFlow {
            request_id,
            name,
            definition,
        } = p;

        // Defensive parse/validation before persisting: at the normal boundary the definition was
        // already validated by `Engine::create_flow`, but a replayed or forged command must not
        // write a definition that `HandlerContext::machine` would later fail to parse. Reads the
        // string, discards the model — the durable record stays the raw string.
        //
        // A malformed definition is the *command's* failure, so it is refused: `dispatch_once` turns
        // the rejection into the command's single response entry — a `Reject` the awaiting caller (and
        // the log) receives as a definitive `INVALID_ARGUMENT`, and the one `log_reject` traces — so
        // there is nothing for this site to record itself. This is the Zeebe model: a
        // well-formed-but-unapplicable command gets a COMMAND_REJECTION, not silence.
        if serde_json::from_str::<spica_asl::StateMachine>(definition).is_err() {
            return Err(ProcessingError::Rejected(
                RejectionType::InvalidArgument,
                format!("create_flow: malformed definition for flow {name}"),
            ));
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
            return Ok(());
        }

        // Assign the durable identity (the handler is the sole identity-assigner): both the flow and
        // this first version mint a never-reused `uid`. Every object carries an independent uid —
        // the name is the addressing key, the uid the sameness/incarnation key (k8s-style). A new
        // flow's first version is always ordinal 1 (its object name `{flow_name}-1`); the flow's
        // `meta.uid` is a distinct fresh ulid from the version's. `created_at` is stamped now and
        // carried on both rows.
        let flow_uid = ctx.mint();
        let flow_version = FlowVersion::version_name(name, 1);
        let flow_version_uid = ctx.mint();
        let created_at = ctx.now();
        // The owner reference both rows are minted against: the flow's plain name plus its fresh uid,
        // bundled once so the flow's own meta and its first version's owner cannot disagree on it.
        let flow_ref = crate::types::meta::ObjectRef::new(
            crate::types::meta::ObjectName::plain(name.as_str())
                .expect("a valid FlowName is a valid user object name"),
            flow_uid,
        );
        tracing::info!(name = %name, uid = %flow_uid, version = %flow_version, "flow with first version created");

        // Emit the flow's birth and its first version in one atomic batch (same cause/stream). The
        // StreamProcessor routes the caller's ack on `FlowVersionCreated` (see `Event::FlowVersionCreated`).
        out.append_event(Event::FlowCreated(FlowCreated {
            request_id: *request_id,
            flow: Flow::new(flow_uid, name, created_at),
        }))
        .await;
        // The CreateFlow caller awaits the request echoed on this `FlowVersionCreated` (the version's
        // birth); an injected `Hook` observer wakes it as soon as the engine reports this event applied.
        let version_event = Event::FlowVersionCreated(FlowVersionCreated {
            request_id: *request_id,
            flow_version: FlowVersion::new(
                flow_version_uid,
                &flow_ref,
                1,
                definition.clone(),
                created_at,
            ),
        });
        out.append_event(version_event).await;

        Ok(())
    }
}
