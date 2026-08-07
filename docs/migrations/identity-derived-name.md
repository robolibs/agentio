# Name-derived identity removal

`IdentitySource::DerivedName` and `derive_secret_from_name` have been removed.
They converted a public label directly into private key material, so anyone who
knew the label could reproduce the identity.

Treat identities created with that API as compromised when their input name
was public. Replace them with randomly generated persistent keys, distribute the
new public endpoint IDs through a trusted channel, and revoke any authorization
granted to the old endpoint IDs. Never copy old private key bytes into reports,
logs, or migration tooling.
