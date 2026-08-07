mod all_exchanges {
    include!("../examples/05_all_exchanges.rs");

    pub(super) fn run() {
        main().unwrap();
    }
}

#[test]
fn all_exchange_families_complete() {
    all_exchanges::run();
}
