# RackRadar

RackRadar is a lightweight IP intelligence service that periodically ingests
regional internet registry (RIR) datasets, stores them in MariaDB/MySQL, and
exposes a tiny HTTP API to query ownership and curated allow/deny lists. The
service downloads RPSL and ARIN bulk WHOIS feeds on a schedule, normalizes
network blocks into relational tables, rebuilds aggregated ranges, and serves
lookup responses over libmicrohttpd.

## Features

- Imports multiple RIR sources (RPSL and ARIN) on configurable intervals and
  tracks serials per registrar.
- Normalizes organizations and IPv4/IPv6 netblocks into tables defined in
  `schema/v1.sql`, with helper union tables for merged ranges.
- Builds named lists using include/exclude filters across organization and
  netblock fields and publishes merged list ranges for efficient consumption.
- Provides HTTP endpoints to query one IP, query up to 100 IPs in one request,
  or download the ranges for a configured list.

## Dependencies

RackRadar is written in C and built with CMake. The project relies on:

- libconfig
- MariaDB/MySQL client libraries
- libcurl, zlib, minizip
- Expat, ICU, Iconv
- libmicrohttpd, yyjson
- pthreads

The CMake build links these libraries automatically when present on the
system.【F:CMakeLists.txt†L5-L45】

## Building

1. Install the dependencies above using your system package manager.
2. Configure the build (Debug preset provided):

   ```bash
   cmake --preset RackRadar
   ```
3. Build the binary:

   ```bash
   cmake --build --preset RackRadar
   ```

The resulting executable `RackRadar` is produced in the `build/` directory.

## Database setup

RackRadar expects a MariaDB/MySQL database. For a fresh installation, create a
database and user, then apply `schema/v1.sql` followed by `schema/v7.sql`
through `schema/v9.sql`. The v1 schema contains the changes covered by
migrations v2 through v6:

```bash
set -e
mysql -u <user> -p -e "CREATE DATABASE rackradar CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -u <user> -p rackradar < schema/v1.sql
mysql -u <user> -p rackradar < schema/v7.sql
mysql -u <user> -p rackradar < schema/v8.sql
mysql -u <user> -p rackradar < schema/v9.sql
```

The schema defines tables for registrars, organizations, IPv4/IPv6 netblocks,
indexed email domains and their organization/netblock relationships, union
tables for merged ranges, and list management tables.
【F:schema/v1.sql†L1-L200】【F:schema/v1.sql†L200-L232】

Existing installations must apply each newer schema migration in order before
starting the matching RackRadar binary. Stop RackRadar before applying the
migrations, and leave it stopped until every migration succeeds. In particular,
`schema/v9.sql` backfills the normalized email-domain relationships, removes the
legacy packed email fields, and resets import staging tables, so it must not run
concurrently with an import. Back up the database first: DDL auto-commits, so
this migration cannot be rolled back as one unit. For example, upgrading a v1
database to the current schema requires all eight migrations; start at the next
schema version after the one already installed:

```bash
set -e
sudo systemctl stop rackradar.service
mysql -u <user> -p rackradar < schema/v2.sql
mysql -u <user> -p rackradar < schema/v3.sql
mysql -u <user> -p rackradar < schema/v4.sql
mysql -u <user> -p rackradar < schema/v5.sql
mysql -u <user> -p rackradar < schema/v6.sql
mysql -u <user> -p rackradar < schema/v7.sql
mysql -u <user> -p rackradar < schema/v8.sql
mysql -u <user> -p rackradar < schema/v9.sql
sudo systemctl start rackradar.service
```

Schema migrations are immutable once deployed. Put later schema changes in a
new migration rather than modifying an earlier migration on an upgraded host.

## Configuration

RackRadar loads its configuration from `/etc/rackradar/main.cfg` using
`libconfig`. A sample configuration is provided in `settings.sample`.
Key options include:

- `database`: host, port, user, pass, name, pool size
- `http.port`: listening port for the HTTP API (default 8888)
- `sources`: one or more RIR downloads with `type` (`RPSL` or `ARIN`),
  `frequency` (seconds between imports), `url`, and optional HTTP `user`/`pass`.
- `lists`: named list definitions with optional `include`/`exclude` arrays and
  per-field filters (`ip_netname`, `ip_descr`, `ip_email`, `org_handle`,
  `org_name`, `org_descr`, `org_email`, and `email`).

RackRadar validates addresses embedded in RPSL organization/netblock attribute
values, including remarks, notify, and e-mail values, and in ARIN
organization/netblock comments. It stores only the lowercase domain and
deduplicates domains case-insensitively. `org_email` searches domains associated
with the organization, `ip_email` searches those associated directly with the
netblock, and the generic `email` filter searches either association. These
filters use SQL `LIKE` patterns against domain names, so patterns must omit the
`@` and local part; for example, use `example.com` for an exact domain or
`%.example.com` for its subdomains. Reference-only contact handles, such as
RPSL `abuse-c` and ARIN `pocLinks`, are not dereferenced.

### Example configuration

```cfg
# /etc/rackradar/main.cfg

database:
{
  host: "127.0.0.1";
  port: 3306;
  user: "rackradar";
  pass: "rackradar";
  name: "rackradar";
  pool: 8;
};

http:
{
  port: 8888;
};

sources:
{
  RIPE:
  {
    type     : "RPSL";
    frequency: 86400; # seconds
    url      : "ftp://ftp.ripe.net/ripe/dbase/ripe.db.gz";
  };

  ARIN:
  {
    type     : "ARIN";
    frequency: 86400;
    url      : "https://account.arin.net/public/secure/downloads/bulkwhois?apikey=<API_KEY>";
  };
};

lists:
{
  ExampleProvider:
  {
    include: [];
    exclude: [];
    ip_netname:
    {
      match : [ "EXAMPLE-%" ];
      ignore: [];
    };
    org_name:
    {
      match : [ "Example%" ];
      ignore: [];
    };
    org_email:
    {
      match : [ "example.com" ];
      ignore: [];
    };
    email:
    {
      match : [ "%.example.com" ];
      ignore: [];
    };
  };
};
```

## How it works

1. **Startup**: `main` initializes logging, loads configuration, connects to the
   database, starts the importer, and launches the HTTP server on the configured
   port.【F:src/main.c†L1-L38】
2. **Import loop**: `rr_import_run` continuously iterates over configured
   sources. For each source it checks the last fetch-attempt time and uses HTTP
   validators plus SHA-256 content and parser-configuration hashes to avoid
   parsing unchanged data. Changed data is parsed into staging tables and
   merged atomically into the live registrar, organization, and netblock
   tables. A changed parser/source configuration permits one immediate fetch;
   applying a schema migration alone does not. Failed or interrupted attempts
   remain subject to the configured interval across restarts. Imports run in a
   loop with a one-second sleep between cycles.
3. **Union & list rebuilds**: Imports that change address coverage trigger an
   atomic refresh of the merged union snapshot. Named lists are rebuilt only
   when imported data or their configuration changes, so downstream consumers
   can request current condensed ranges without needless rebuilds.
4. **HTTP API**: The microhttpd server exposes these endpoints:
   - `/ip/<addr>`: return ownership info for an IPv4 or IPv6 address.
   - `POST /ip/bulk`: return JSON ownership results for up to 100 addresses.
   - `/list/v4/<name>`: stream IPv4 CIDRs for a configured list.
   - `/list/v6/<name>`: stream IPv6 CIDRs for a configured list.

   The handlers look up data using prepared DB queries and respond with plain
   text payloads or standard HTTP error codes.【F:src/http.c†L24-L220】【F:src/http.c†L223-L320】

## Running RackRadar

1. Ensure `/etc/rackradar/main.cfg` is present and the database is reachable.
2. Start the binary from the build directory:

   ```bash
   ./RackRadar
   ```

3. Query the API:

   ```bash
   # Lookup a single address
   curl http://localhost:8888/ip/8.8.8.8

   # Lookup several addresses with one database connection and HTTP request
   curl -H 'Content-Type: application/json' \
     --data '{"ips":["8.8.8.8","2001:4860:4860::8888"]}' \
     http://localhost:8888/ip/bulk

   # Download the IPv4 ranges for a list named "ExampleProvider"
   curl http://localhost:8888/list/v4/ExampleProvider
   ```

The bulk endpoint accepts a JSON object whose only member is `ips`, an array of
1 to 100 unique, valid IP addresses as strings. The request body is limited to
16 KiB. Its `results` array preserves request order; each item contains the
input `ip` and a `found` flag.
Found items also include `netblock`, `netname`, `org_handle`, `org_name`, and
`descr`. Invalid requests are rejected as a whole, while valid addresses with
no matching allocation are returned with `"found": false`. Malformed requests,
oversized bodies, unsupported media types, and database failures return 400,
413, 415, and 500 respectively; methods other than POST return 405.

`tools/rackradar_abuse_report.py` uses `/ip/bulk` in batches of 100 by
default, applies the configured request-rate limit per batch, and falls back to
the legacy single-address endpoint when bulk lookup is unavailable. Use
`--batch-size` to reduce the batch size or `--no-bulk` to force legacy lookup.

RackRadar logs import progress and HTTP errors to stdout/stderr by default via
its logging subsystem.
