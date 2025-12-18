# qtql

I got a little bored of writing services and routes, and thought why not try to code something different.

So I started doing some IO drills in Go. That gave me an idea to write a WAL, which turned into a key-value store (again).

I also read the [Volcano paper](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf) last week, so I thought why not build a Volcano-style query engine into it.

So yeah, here it is. **qtql** :)

## What is this?

A key-value store with:
- WAL (Write-Ahead Log) with CRC32 verification
- TTL support for keys
- A query engine using the Volcano iterator model
- SQL-ish query syntax

## The Volcano Model

From Graefe's 1994 paper: queries are trees of operators, each with `Open()`, `Next()`, `Close()`. Data flows up through `Next()` calls - pull-based, lazy evaluation, memory efficient.

```
         ┌─────────┐
         │  Limit  │  ← Next() returns nil after N rows
         └────┬────┘
              │ pulls from
         ┌────▼────┐
         │ Filter  │  ← Next() skips non-matching rows
         └────┬────┘
              │ pulls from
         ┌────▼────┐
         │ KVScan  │  ← Next() returns one row at a time
         └─────────┘
```

## Usage

```bash
go build .
./qtql
```

## Commands

```
SET key value [ttl]     # SET user:1 alice 5m
GET key                 # GET user:1
DELETE key              # DELETE user:1
EXPIRE key ttl          # EXPIRE user:1 10m
TTL key                 # TTL user:1
EXISTS key              # EXISTS user:1
HYDRATE                 # Load sample data for testing
```

## Query Engine

```
SCAN                                        # all keys
SCAN LIMIT 10                               # first 10
SCAN SELECT key                             # keys only (projection)
SCAN SELECT *                               # both key and value (default)
SCAN WHERE key LIKE user:*                  # glob pattern on key
SCAN WHERE value CONTAINS error             # substring match on value
SCAN WHERE key LIKE user:* LIMIT 5          # combine clauses
SCAN SELECT key WHERE key LIKE order:*      # projection + filter
```

Example:
```
qtql > HYDRATE
Hydrated store with 16 sample entries

qtql > SCAN WHERE key LIKE user:* LIMIT 2

Query Plan:
───────────
→ Limit (max=2)
  → Filter (KEY LIKE "user:*")
    → KVScan

Results (2 rows):
  user:1: alice
  user:2: bob
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     CLI (main.go)                   │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│                KV Store (kv_store.go)               │
│  • In-memory map with RWMutex                       │
│  • WAL with CRC32 checksums                         │
│  • TTL/expiration support                           │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│              Query Planner (executor.go)            │
│  • Parses SQL-ish syntax                            │
│  • Builds operator tree (filters before limit)      │
│  • Prints query plan                                │
└───────────────────────┬─────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────┐
│            Volcano Operators (operator.go)          │
│  • KVScan  - full table scan                        │
│  • Filter  - predicate evaluation                   │
│  • Limit   - early termination                      │
│  • Project - column selection                       │
└─────────────────────────────────────────────────────┘
```

## WAL Format

Each line: `<command>|<crc32>`

```
SET user:1 alice|a1b2c3d4
DELETE user:2|deadbeef
EXPIRE user:1 5m0s|cafebabe
```

On startup, WAL is replayed. Corrupted entries (CRC mismatch) are rejected.

## Files

```
main.go       - CLI entry point
kv_store.go   - Store, WAL, commands
operator.go   - Volcano operators (Scan, Filter, Limit, Project)
executor.go   - Query parser, planner, executor
```

## What I learned

1. Volcano's pull-based model is elegant - each operator is simple, composable
2. WAL + CRC gives you durability with corruption detection
3. Query planners are just tree builders with ordering rules
4. Go's interfaces make the operator pattern clean

## References

- [Volcano - An Extensible and Parallel Query Evaluation System](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf) - Graefe, 1994
- [How Query Engines Work](https://howqueryengineswork.com/) - Andy Grove
