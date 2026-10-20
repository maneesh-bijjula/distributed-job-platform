# 100 real fault injections — raw evidence

The recorded campaign passed all 100 planned trials (20 per fault kind), with
800/800 accepted jobs successful and zero observed loss, incorrect results or
duplicate successful attempts. Source revision:
`92bc48a14276b8b94555cdc0473ac71679679491`.

`report.json` is the final verdict after recovery, population reconciliation and
cleanup. `environment.json` records the exact source/image identities and resource
settings. `plan.json` is the immutable pre-run declaration, retaining its initial
status; use `report.json` for the final outcome.

`raw-evidence.tar.gz` contains all 871 original files with relative paths:
the append-only fault journal, 100 trial reports, workload request/job/attempt/event
and recovery-replay ledgers, population reconciliation, correlated stale-write
rejection logs, resources and final container states. Payload contents and raw
results were removed only after live independent-oracle verification. No API keys,
passwords, plaintext job inputs, database archives or private configuration files
are included. All files, including decompressed gzip contents, were scanned against
the generated local credential values before publication.

`audit.json` records separate ledger/checksum checks, archive SHA-256 and
individual raw-file checksums. `SHA256SUMS` covers this published directory.
Extract into an empty directory to inspect individual files. Hashes detect changes;
they do not provide a third-party signature or hardware attestation.

The five-trial pilot and ordinary CI regression are separate runs and are not
counted toward these 100 injections. See [results and limits](../../fault-campaign-results.md)
and [the method](../../fault-campaign.md) before using a numerical claim.
