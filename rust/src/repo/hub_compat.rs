//! Adapt samod's connector and streaming-search APIs to the existing Python API.
//!
//! Python supplies already-established transports and expects find_document to
//! complete only once the document is ready (or currently unavailable).

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use samod_core::actors::hub::{CommandResult, Hub, HubEvent, HubResults};
use samod_core::network::{ConnectionEvent, ConnectionOwner};
use samod_core::{
    CommandId, DocSearch, DocSearchPhase, DocumentActorId, DocumentId, UnixTimestamp,
};

use super::hub_events::HubEventKind;

type SearchWaiters = HashMap<DocumentId, Vec<(CommandId, DocumentActorId)>>;

pub(crate) struct CompatHub {
    hub: Hub,
    pending_searches: SearchWaiters,
}

impl Deref for CompatHub {
    type Target = Hub;

    fn deref(&self) -> &Hub {
        &self.hub
    }
}

impl CompatHub {
    pub(crate) fn new(hub: Hub) -> Self {
        Self {
            hub,
            pending_searches: HashMap::new(),
        }
    }

    pub(crate) fn handle_event<R: rand::Rng>(
        &mut self,
        rng: &mut R,
        now: UnixTimestamp,
        event: &HubEventKind,
    ) -> Result<HubResults, String> {
        let mut results = match event {
            HubEventKind::Core(event) | HubEventKind::FindDocument { event, .. } => {
                self.hub.handle_event(rng, now, event.clone())
            }
            HubEventKind::CreateConnection { command_id, event } => {
                self.create_connection(rng, now, *command_id, event.clone())?
            }
        };

        // Connectors are single-use: Python owns the transport lifecycle and
        // does not provide samod with a transport factory for reconnection.
        if !results.stopped {
            let failed_owners: HashSet<_> = results
                .connection_events
                .iter()
                .filter_map(|event| match event {
                    ConnectionEvent::ConnectionFailed { owner, .. } => Some(*owner),
                    _ => None,
                })
                .collect();
            for owner in failed_owners {
                let event = match owner {
                    ConnectionOwner::Dialer(id) => HubEvent::remove_dialer(id),
                    ConnectionOwner::Listener(id) => HubEvent::remove_listener(id),
                };
                merge_results(&mut results, self.hub.handle_event(rng, now, event));
            }
        }

        if let HubEventKind::FindDocument {
            command_id,
            document_id,
            ..
        } = event
        {
            if let Some(CommandResult::SearchForDoc {
                actor_id,
                search_state,
            }) = results.completed_commands.get(command_id)
            {
                if !search_complete(search_state) {
                    self.pending_searches
                        .entry(document_id.clone())
                        .or_default()
                        .push((*command_id, *actor_id));
                    results.completed_commands.remove(command_id);
                }
            }
        }

        for (document_id, search_state) in &results.search_state_updates {
            if search_complete(search_state) {
                if let Some(waiters) = self.pending_searches.remove(document_id) {
                    for (command_id, actor_id) in waiters {
                        results.completed_commands.insert(
                            command_id,
                            CommandResult::SearchForDoc {
                                actor_id,
                                search_state: search_state.clone(),
                            },
                        );
                    }
                }
            }
        }
        if results.stopped {
            self.pending_searches.clear();
        }
        Ok(results)
    }

    fn create_connection<R: rand::Rng>(
        &mut self,
        rng: &mut R,
        now: UnixTimestamp,
        command_id: CommandId,
        registration: HubEvent,
    ) -> Result<HubResults, String> {
        let mut results = self.hub.handle_event(rng, now, registration);
        let connection = match results.completed_commands.remove(&command_id) {
            Some(CommandResult::AddDialer { dialer_id }) => {
                // No authenticated peer identity is provided by the Python API.
                HubEvent::create_dialer_connection(dialer_id, None)
            }
            Some(CommandResult::AddListener { listener_id }) => {
                HubEvent::create_listener_connection(listener_id, None)
            }
            _ => return Err("Could not register transport connector".into()),
        };
        // The initial dial request is already satisfied by the supplied transport.
        results.dial_requests.clear();
        let mut connected = self.hub.handle_event(rng, now, connection.event);
        let result = connected
            .completed_commands
            .remove(&connection.command_id)
            .ok_or("Could not create transport connection")?;
        connected.completed_commands.insert(command_id, result);
        merge_results(&mut results, connected);
        Ok(results)
    }
}

fn search_complete(search: &DocSearch) -> bool {
    matches!(search.phase(), DocSearchPhase::Ready) || search.is_currently_unavailable()
}

fn merge_results(results: &mut HubResults, mut next: HubResults) {
    results.new_tasks.append(&mut next.new_tasks);
    results.completed_commands.extend(next.completed_commands);
    results.spawn_actors.append(&mut next.spawn_actors);
    results.actor_messages.append(&mut next.actor_messages);
    results
        .connection_events
        .append(&mut next.connection_events);
    results.dial_requests.append(&mut next.dial_requests);
    results.dialer_events.append(&mut next.dialer_events);
    results
        .search_state_updates
        .append(&mut next.search_state_updates);
    results.stopped |= next.stopped;
    results.connections_count = next.connections_count;
    results.documents_count = next.documents_count;
}
