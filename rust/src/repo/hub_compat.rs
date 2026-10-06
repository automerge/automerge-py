//! Aggregate samod's streaming-search updates for the existing Python find API.

use std::collections::HashMap;
use std::ops::Deref;

use samod_core::actors::hub::{CommandResult, Hub, HubResults};
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
        };

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
}

fn search_complete(search: &DocSearch) -> bool {
    matches!(search.phase(), DocSearchPhase::Ready) || search.is_currently_unavailable()
}
