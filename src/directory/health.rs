use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectoryHealth {
    pub successful_announcements: u64,
    pub announcement_failures: u64,
    pub resolver_errors: u64,
    pub rejected_records: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ControlPlaneHealth {
    successful_announcements: AtomicU64,
    announcement_failures: AtomicU64,
    resolver_errors: AtomicU64,
    rejected_records: AtomicU64,
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

    pub(crate) fn snapshot(&self) -> DirectoryHealth {
        DirectoryHealth {
            successful_announcements: self.successful_announcements.load(Ordering::Relaxed),
            announcement_failures: self.announcement_failures.load(Ordering::Relaxed),
            resolver_errors: self.resolver_errors.load(Ordering::Relaxed),
            rejected_records: self.rejected_records.load(Ordering::Relaxed),
        }
    }
}
