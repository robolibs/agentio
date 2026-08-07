use std::ops::{Deref, DerefMut};
use std::sync::Weak;

use crate::directory::TopicEntry;

use super::core::{AgentInner, withdraw_owned_entry};

/// A hosted peerbus handle that withdraws its signed directory record on drop.
pub struct Registered<H> {
    handle: H,
    entry: TopicEntry,
    agent: Weak<AgentInner>,
}

impl<H> Registered<H> {
    pub(crate) fn new(handle: H, entry: TopicEntry, agent: Weak<AgentInner>) -> Self {
        Self {
            handle,
            entry,
            agent,
        }
    }
}

impl<H> Deref for Registered<H> {
    type Target = H;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<H> DerefMut for Registered<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

impl<H> Drop for Registered<H> {
    fn drop(&mut self) {
        if let Some(agent) = self.agent.upgrade() {
            withdraw_owned_entry(&agent, &self.entry);
        }
    }
}
