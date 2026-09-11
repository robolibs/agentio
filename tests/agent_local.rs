mod all_exchanges {
    include!("../examples/05_all_exchanges.rs");

    pub(super) fn run_local() {
        main().unwrap();
    }

    pub(super) fn run_remote() {
        run(true).unwrap();
    }

    pub(super) fn rejects_tampered_count() {
        assert!(!payload_matches(100, &[1, 1], 1));
    }
}

#[test]
fn all_exchange_families_complete() {
    all_exchanges::run_local();
}

#[test]
fn all_exchange_families_complete_over_quic() {
    all_exchanges::run_remote();
}

#[test]
fn claimed_payload_size_cannot_inflate_measurement() {
    all_exchanges::rejects_tampered_count();
}
