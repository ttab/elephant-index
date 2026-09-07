# Elephant index

Elephant index follows the event log from the [elephant
repository](https://github.com/ttab/elephant-api), flattens each updated
document into a flat property structure, and indexes it into OpenSearch. It
serves search over what it has indexed, and a management API for the index
sets and the OpenSearch clusters they live in.

Before indexing, a document is flattened to a [flat property
structure](index/testdata/raw_1.values.json), and the
[revisor](https://github.com/ttab/revisor) schemas from the repository are
combined with the document data to construct the [OpenSearch index
mappings](index/testdata/raw_1.mappings.json). The indexer extends the
mappings as needed, and **no property is indexed before a mapping exists for
it**. A separate index is created per document type and language, which avoids
mapping conflicts between types and lets each index use a language-specific
analyzer.

Sitting on top of that is percolation: a search can register a subscription,
and newly indexed documents are matched against the stored subscription
queries so clients can long-poll for changes. Subscriptions are explicitly
best-effort — see [Delivery
guarantees](docs/architecture.md#delivery-guarantees-there-are-none).

## Documentation

| Document | What it settles |
|---|---|
| **README.md** (this document) | Orientation and the working reference: layout, build, how to run it, every configuration flag. |
| [docs/architecture.md](docs/architecture.md) | How the service is built: process model, data flow, subsystems, API surface. |
| [docs/ops.md](docs/ops.md) | The operator's view: dependencies, ports, bootstrap order, failure modes and their signals. |
| [docs/observability.md](docs/observability.md) | Every metric the service exports and what a change in it means. |

The documents link to each other by heading, and a renamed heading breaks a
link silently, so the links are checked mechanically:

``` shell
mage docs:links
```

That runs in CI alongside the linter.

## Repository layout

```
cmd/index/          CLI entry point; the single `run` command
index/              all core logic
  server.go           RunIndex: starts the goroutines, mounts both RPC stacks
  coordinator.go      index set lifecycle, notification fan-out, percolator owner
  index.go            per-index-set indexer following the event log
  index_worker.go     per type × language batching and bulk writes
  build.go            document flattening
  mappings.go         mapping construction from revisor schemas
  language-settings.go  language-specific ICU analyzers
  search_api.go       the SearchV1 service
  management_api.go   the Management service
  percolator.go       subscription matching and delivery
  osclient.go         cluster-aware OpenSearch client provider
  aes.go              cluster password encryption
  proxy.go            legacy elastic `_search` proxy, marked for retirement
internal/           HTTP helpers and search request parsing/translation
postgres/           sqlc-generated database layer — do not edit by hand
schema/             tern migrations, including the vendored job lock migration
scripts/            operational helpers
testdata/           golden files and test documents
```

## Build & development tools

The toolchain floor is in `go.mod` (currently Go 1.27.1); `GOTOOLCHAIN=auto`
fetches it.

``` shell
go build -o /dev/null ./...        # compile check, leaves nothing behind
go run ./cmd/index run             # run it (see below for what it needs)
golangci-lint run --timeout=4m     # lint, as CI does
mage docs:links                    # documentation link check, as CI does
go test ./...                      # full suite, needs Docker
```

Generated code and migrations go through the docker images `ttab/mage` pins;
do not install the generators locally.

``` shell
mage sql:migrate                   # apply migrations to the local database
mage sql:generate                  # regenerate postgres/ from queries.sql
mage sql:vendor                    # copy in library migrations not yet taken
mage sql:vendorCheck               # fail if a library migration is missing
```

### Tests

The suite is integration tests: `eltest` starts Postgres, OpenSearch, Minio, a
mock OIDC provider and a real `elephant-repository` container per test, so
**Docker is required and the host's own Postgres is never touched**.

The service serves Twirp and Connect from one implementation, so the suite runs
against either stack:

``` shell
go test ./...                            # Twirp, the default
TEST_RPC_STACK=connect go test ./...     # Connect
REGENERATE=true go test ./index -run TestWireShape   # refresh golden files
```

CI runs both. Wire-shape golden files under `testdata/TestWireShape*` pin the
success and error bodies per stack, so a change in either encoding is a
visible diff.

## Running a local dev instance

Bring it up piecewise; each step says what the service does without it.

1. **Postgres.** There is usually one running on the development machine.
   `mage sql:db` creates the database and role, `mage sql:migrate` applies the
   schema. **Without a migrated database nothing starts.**
2. **An OpenSearch cluster.** Register it through the management API once the
   service is up. Without one, the service starts, is ready, and serves
   nothing.
3. **The repository.** `--repository-endpoint` must point at a running
   elephant repository; the revisor schemas are loaded from it at startup, so
   **the service does not start without it**.
4. **Credentials.** The service authenticates against a real OIDC provider
   even locally. Run it through `ttrun` so the client credentials resolve:

   ``` shell
   ttrun -- go run ./cmd/index run
   ```

5. **A password key.** Required, 32 bytes hex. See [Password encryption
   key](#password-encryption-key).

### Resetting a local dev environment

``` shell
mage sql:dropDB && mage sql:db && mage sql:migrate
```

The OpenSearch indices are derived, so dropping the database and re-registering
a cluster is a clean slate. Leftover `documents-*` indices from a previous run
are orphaned and can be deleted directly.

## Password encryption key

The service requires a 32-byte hex-encoded key (`--password-key`, or
`PASSWORD_ENCRYPTION_KEY`) used to encrypt OpenSearch cluster passwords before
storing them. Passwords are encrypted with AES-256-GCM and stored in a
versioned envelope, `v1.<base64>`.

The key is used when registering a cluster with password authentication, when
parsing credentials out of the `--opensearch-endpoint` URL, and when building a
client for a stored cluster.

> **Tip:** generate one with `head -c 32 /dev/urandom | xxd -ps -cols 0`, or
> use `scripts/set-encryption-key` to write one to Vault.

**The key cannot be rotated without re-registering every cluster.** Nothing
re-encrypts stored passwords under a new key, so a changed key turns every
stored cluster password into an undecryptable blob — and it fails when a
client is built, not at startup.

## Configuration reference

### Server

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--addr` | `ADDR` | `:1080` | API listener: both RPC path families and the elastic proxy. |
| `--profile-addr` | `PROFILE_ADDR` | `:1081` | Metrics, health and pprof. Keep it off the API port so a probe cannot be answered by the proxy. |
| `--tls-addr` | `TLS_ADDR`, `TLS_LISTEN_ADDR` | `:1443` | TLS listener, used only when a certificate is set. |
| `--cert-file` | `TLS_CERT_PATH` | — | Enables the TLS listener. |
| `--key-file` | `TLS_KEY_PATH` | — | Must be set with `--cert-file`. |
| `--log-level` | `LOG_LEVEL` | `debug` | Raise it in anything shared; the indexer is chatty at debug. |
| `--cors-host` | `CORS_HOSTS` | — | Allowed CORS hosts, wildcards supported. The Connect request headers are allowed by default, so browser Connect clients need nothing extra here. |

### Dependencies

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--db` | `CONN_STRING` | `postgres://elephant-index:pass@localhost/elephant-index` | The default is for local development only. **Set `pool_max_conns` explicitly in any hosted environment** — pgx otherwise sizes the pool from the node's CPU count, which changes invisibly on reschedule. |
| `--db-parameter` | `CONN_STRING_PARAMETER` | — | Reads the connection string from a parameter source instead. |
| `--repository-endpoint` | `REPOSITORY_ENDPOINT` | — | **Required.** The event log, documents and revisor schemas. |
| `--parameter-source` | `PARAMETER_SOURCE` | — | Where `*-parameter` flags resolve from. |
| `--password-key` | `PASSWORD_ENCRYPTION_KEY` | — | **Required**, 32 bytes hex. See above. |

### Indexing

The master switch is `--no-indexer`: with it set the process is a read-only
search frontend and none of the indexing below runs.

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--no-indexer` | `NO_INDEXER` | `false` | Serve search only. The coordinator builds a client for the active set and starts no indexers. |
| `--default-language` | `DEFAULT_LANGUAGE` | — | **Required.** The language assumed for a document that does not declare one. Required only because the repository does not yet enforce that documents carry a language; it should stop being required once it does. |
| `--sharding-policy` | `SHARDING_POLICY` | 2 shards, 2 replicas | Comma-separated `prefix:shards:replicas` stanzas, most specific match winning — `:1:2,core_article-:2:2,core_article-sv-se:5:2`. The empty prefix is the default. |
| `--opensearch-endpoint` | `OPENSEARCH_ENDPOINT` | — | Intended to name a default cluster to create an index set in. **It does not currently do that** — see [What is not in place yet](docs/ops.md#what-is-not-in-place-yet). Credentials in its userinfo are still read and encrypted. |
| `--managed-opensearch` | `MANAGED_OPENSEARCH` | `false` | Sign OpenSearch requests with AWS IAM instead of using a username and password. |

Authentication flags come from `elephantine.AuthenticationCLIFlags()` and are
the fleet-standard set. The service requests `eventlog_read`, `doc_read_all`
and `schema_read` for its own background work.

## Index names

Indices are named `documents-<index set>-<type>-<language>`. Index set
"factual-tiger", type `core/article` and language `sv-se` gives
`documents-factual-tiger-core_article-sv-se`. A language code without a region
gets the suffix `-unspecified`, so `sv` gives
`documents-factual-tiger-core_article-sv-unspecified`.

## Re-indexing

Re-indexing creates a new index set and indexes every document into it. The new
set can live in a different cluster to avoid degrading the active one.
Activation is a read-path switch, so it is reversible until the old set is
deleted.

``` http
POST /twirp/elephant.index.Management/Reindex

{"cluster":"emerging-stranger"}

HTTP/1.1 200 OK

{
  "name": "magical-cottonmouth"
}
```

Indexing into "magical-cottonmouth" starts immediately. Check progress through
the metrics or by listing the index sets:

``` http
POST /twirp/elephant.index.Management/ListIndexSets

{}

HTTP/1.1 200 OK

{
  "index_sets": [
    {
      "name": "awake-blockbuster",
      "cluster": "emerging-stranger",
      "enabled": true,
      "active": true,
      "position": "4263034"
    },
    {
      "name": "magical-cottonmouth",
      "cluster": "emerging-stranger",
      "enabled": true,
      "position": "372242"
    }
  ]
}
```

`SetIndexSetStatus` pauses indexing into a set (`enabled: false`) or makes it
active:

``` http
POST /twirp/elephant.index.Management/SetIndexSetStatus

{
  "name": "magical-cottonmouth",
  "active": true
}

HTTP/1.1 200 OK

{}
```

Activating a set that lags the active one by more than 10 events is refused
unless `force_active` is set:

``` http
HTTP/1.1 412 Precondition Failed

{
  "code": "failed_precondition",
  "msg": "the index set lags behind with more than 10 events (3399619), use force_active to activate anyway"
}
```

**On the Connect path that same refusal is `400`, not `412`**, and the body
spells the message under `message` rather than `msg`. Read the code from the
body rather than keying on the status.

Once the new set is active and verified, disable the old one and delete it:

``` http
POST /twirp/elephant.index.Management/DeleteIndexSet

{
  "name": "awake-blockbuster"
}

HTTP/1.1 200 OK

{}
```

Deletion is the only irreversible step, so verify before taking it.

The same requests on the Connect paths drop the `/twirp` prefix — `POST
/elephant.index.Management/ListIndexSets` — and spell response fields in
lowerCamelCase (`indexSets`). See [the API
surface](docs/architecture.md#api-surface).

## Upgrading OpenSearch

We lost documents and indices doing an in-place blue/green upgrade of a cluster
from v2.5 to v2.19 in stage. The cause was never established; on-demand index
creation is the likely culprit.

**Do not upgrade a cluster under a live index set.** Create a new cluster on
the new version, register it, re-index into a set in that cluster, and switch
when it has caught up. That is reversible, which an in-place upgrade is not.

## Pending work

**`--opensearch-endpoint` does not create a default index set.** The URL is
parsed into a variable shadowed inside an `if` block in `cmd/index/main.go`, so
`DefaultCluster` reaches `RunIndex` as nil and `EnsureDefaultIndexSet` never
runs. A fresh deployment has to register a cluster and create an index set
through the management API. The test suite passes `DefaultCluster` directly and
does not cover the path.

**Two RPC methods are mounted but panic.** `SearchV1.EndSubscription` and
`Management.PartialReindex` both `panic("unimplemented")`;
`EndSubscription` does so before checking a scope, so any caller holding a
valid token reaches it. Subscriptions are reaped by age instead.

**`elephant_indexer_mapping_update_total` is never incremented.** It is
registered and permanently zero.

**The elastic proxy is marked for retirement** but still mounted on `/`, and
still what some clients use.

**Percolation is serial and unbatched**, so it is the first thing to lag under
load, and it lags silently. The options — language-neutral percolator indices,
batching, per-type concurrency, shedding under high throughput — are written
up in [architecture.md](docs/architecture.md#pending-work).

**Twirp is still served and still the default.** Removing it waits for the
next major release; the migration state is
[architecture.md](docs/architecture.md#api-surface).
