use std::ops::{Deref, DerefMut};
use std::sync::Weak;

use crate::directory::TopicEntry;

use super::core::{AgentInner, withdraw_owned_entry};

/// Withdraws a hosted topic's signed directory record when dropped.
pub struct HostedGuard {
    entry: TopicEntry,
    agent: Weak<AgentInner>,
}

impl HostedGuard {
    /// The record this guard withdraws.
    pub fn entry(&self) -> &TopicEntry {
        &self.entry
    }
}

impl Drop for HostedGuard {
    fn drop(&mut self) {
        if let Some(agent) = self.agent.upgrade() {
            withdraw_owned_entry(&agent, &self.entry);
        }
    }
}

/// A hosted peerbus handle that withdraws its signed directory record on drop.
pub struct Registered<H> {
    handle: H,
    guard: HostedGuard,
}

impl<H> Registered<H> {
    pub(crate) fn new(handle: H, entry: TopicEntry, agent: Weak<AgentInner>) -> Self {
        Self {
            handle,
            guard: HostedGuard { entry, agent },
        }
    }

    /// The signed record this handle hosts.
    pub fn entry(&self) -> &TopicEntry {
        &self.guard.entry
    }

    /// Split the peerbus handle from the record guard, for wrappers that
    /// must own the bare handle; the record stays hosted until the guard
    /// drops.
    pub fn into_parts(self) -> (H, HostedGuard) {
        (self.handle, self.guard)
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
