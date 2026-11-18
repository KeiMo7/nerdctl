# nerdctl compose

| :zap: Requirement | nerdctl >= 0.8 |
|-------------------|----------------|

## Usage

The `nerdctl compose` CLI is designed to be compatible with `docker-compose`.

```console
$ nerdctl compose up -d
$ nerdctl compose down
```

## Dependency-aware startup

`nerdctl compose up` builds the service graph from `depends_on` (plus
`network_mode: service:...` and `volumes_from` edges), not from the order of
entries in the Compose file. Services in the same topological level are started
in parallel, and dependents start only after their dependencies are running.
When a dependency declares a healthcheck, dependents wait for `healthy` before
starting.

Networks and named volumes needed by the selected service graph are created
before the first service starts. If startup fails or is cancelled, containers
started by that invocation are removed in reverse dependency order, followed by
the networks and volumes created by that invocation. Services and resources
that were already present when the command started are left untouched.

Startup and rollback events are logged with UTC timestamps and their
topological level, for example:

```console
[compose-plan] time=... order=L1=[cache,db] L2=[app]
[compose-startup] time=... level=1 service="db" event=ready
[compose-rollback] time=... action=summary rolled-back=[app, db, cache, network:...]
```

Shared networks and named volumes that connect services without an explicit
`depends_on` are reported as warnings. They do not infer an ordering edge.

See the Command Reference in [`../README.md`](../README.md).

## Spec conformance

`nerdctl compose` implements [The Compose Specification](https://github.com/compose-spec/compose-spec),
which was derived from [Docker Compose file version 3 specification](https://docs.docker.com/compose/compose-file/compose-file-v3/).

### Unimplemented YAML fields
- Fields that correspond to unimplemented `docker run` flags, e.g., `services.<SERVICE>.links` (corresponds to `docker run --link`)
- Fields that correspond to unimplemented `docker build` flags, e.g., `services.<SERVICE>.build.extra_hosts` (corresponds to `docker build --add-host`)
- `services.<SERVICE>.credential_spec`
- `services.<SERVICE>.deploy.update_config`
- `services.<SERVICE>.deploy.rollback_config`
- `services.<SERVICE>.deploy.placement`
- `services.<SERVICE>.deploy.endpoint_mode`
- `configs.<CONFIG>.external`
- `secrets.<SECRET>.external`

### Incompatibility
#### `services.<SERVICE>.build.context`
- The value must be a local directory path, not a URL.

#### `services.<SERVICE>.secrets`, `services.<SERVICE>.configs`
- `uid`, `gid`: Cannot be specified. The default value is not propagated from `USER` instruction of Dockerfile.
  The file owner corresponds to the original file on the host.
- `mode`: Cannot be specified. The file is mounted as read-only, with permission bits that correspond to the original file on the host.

#### `services.<SERVICE>.volumes[].type: image`
- Whole-image mounts are supported.
- `source` is interpreted as an image reference, including when it matches a service name.
- `services.<SERVICE>.volumes[].image.subpath` is not yet supported.
