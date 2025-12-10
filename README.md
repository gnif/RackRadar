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
- Provides HTTP endpoints to query a single IP or download the ranges for a
  configured list.

## Dependencies

RackRadar is written in C and built with CMake. The project relies on:

- libconfig
- MariaDB/MySQL client libraries
- libcurl, zlib, minizip
- Expat, ICU, Iconv
- libmicrohttpd
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

RackRadar expects a MariaDB/MySQL database. Create a database and user, then
apply the schema:

```bash
mysql -u <user> -p -e "CREATE DATABASE rackradar CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -u <user> -p rackradar < schema/v1.sql
```

The schema defines tables for registrars, organizations, IPv4/IPv6 netblocks,
union tables for merged ranges, and list management tables.【F:schema/v1.sql†L1-L200】【F:schema/v1.sql†L200-L232】

## Configuration

RackRadar loads its configuration from `/etc/rackradar/main.cfg` using
`libconfig`. A sample configuration is provided in `settings.sample`.
Key options include:

- `database`: host, port, user, pass, name, pool size
- `http.port`: listening port for the HTTP API (default 8888)
- `sources`: one or more RIR downloads with `type` (`RPSL` or `ARIN`),
  `frequency` (seconds between imports), `url`, and optional HTTP `user`/`pass`.
- `lists`: named list definitions with optional `include`/`exclude` arrays and
  per-field filters (`ip.netname`, `ip.descr`, `org.handle`, `org.name`,
  `org.descr`).【F:settings.sample†L1-L56】

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
  };
};
```

## How it works

1. **Startup**: `main` initializes logging, loads configuration, connects to the
   database, starts the importer, and launches the HTTP server on the configured
   port.【F:src/main.c†L1-L38】
2. **Import loop**: `rr_import_run` continuously iterates over configured
   sources. For each source it checks the last import time, downloads the data
   (with optional HTTP auth), parses it via the RPSL or ARIN importer, updates
   registrars/organizations/netblocks, and logs per-import statistics. Imports
   run in a loop with a one-second sleep between cycles.【F:src/import.c†L964-L1164】
3. **Union & list rebuilds**: Successful imports trigger recomputation of the
   merged union tables and any configured named lists so downstream consumers
   can request condensed ranges.【F:src/import.c†L920-L1002】【F:src/import.c†L1127-L1156】
4. **HTTP API**: The microhttpd server exposes GET endpoints:
   - `/ip/<addr>`: return ownership info for an IPv4 or IPv6 address.
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

   # Download the IPv4 ranges for a list named "ExampleProvider"
   curl http://localhost:8888/list/v4/ExampleProvider
   ```

RackRadar logs import progress and HTTP errors to stdout/stderr by default via
its logging subsystem.
