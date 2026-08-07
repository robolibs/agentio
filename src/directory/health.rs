use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectoryHealth {
    pub successful_announcements: u64,
    pub announcement_failures: u64,
    pub resolver_errors: u64,
    pub rejected_records: u64,
    pub last_successful_reconciliation_ms: u64,
    pub stale_seeds: u64,
    pub conflicts: u64,
    pub pending_announcements: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ControlPlaneHealth {
    successful_announcements: AtomicU64,
    announcement_failures: AtomicU64,
    resolver_errors: AtomicU64,
    rejected_records: AtomicU64,
    last_successful_reconciliation_ms: AtomicU64,
    stale_seeds: AtomicU64,
    conflicts: AtomicU64,
    pending_announcements: AtomicU64,
}

impl ControlPlaneHealth {
    pub(crate) fn announcement_succeeded(&self) {
        self.successful_announcements
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn announcement_failed(&self) {
        self.announcement_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn resolver_error(&self) {
        self.resolver_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reject_record(&self) {
        self.rejected_records.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reconciled(&self, timestamp_ms: u64, stale_seeds: u64) {
        self.last_successful_reconciliation_ms
            .store(timestamp_ms, Ordering::Relaxed);
        self.stale_seeds.store(stale_seeds, Ordering::Relaxed);
    }

    pub(crate) fn conflict(&self) {
        self.conflicts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_pending_announcements(&self, pending: u64) {
        self.pending_announcements.store(pending, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> DirectoryHealth {
        DirectoryHealth {
            successful_announcements: self.successful_announcements.load(Ordering::Relaxed),
            announcement_failures: self.announcement_failures.load(Ordering::Relaxed),
            resolver_errors: self.resolver_errors.load(Ordering::Relaxed),
            rejected_records: self.rejected_records.load(Ordering::Relaxed),
            last_successful_reconciliation_ms: self
                .last_successful_reconciliation_ms
                .load(Ordering::Relaxed),
            stale_seeds: self.stale_seeds.load(Ordering::Relaxed),
            conflicts: self.conflicts.load(Ordering::Relaxed),
            pending_announcements: self.pending_announcements.load(Ordering::Relaxed),
        }
    }
}
