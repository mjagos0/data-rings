# Third-party licenses

This directory collects the copyright notices and licence texts of the
third-party libraries this project links against. Each subdirectory
contains a verbatim copy of the licence file(s) shipped by the
corresponding upstream project.

All listed libraries are distributed under permissive, OSI-approved
licences that are compatible with the MIT licence applied to this
project (see `../../LICENSE`).

## Direct dependencies

| Library | Import path | Licence |
| --- | --- | --- |
| boxo | `github.com/ipfs/boxo` | Apache-2.0 OR MIT |
| go-cid | `github.com/ipfs/go-cid` | MIT |
| go-block-format | `github.com/ipfs/go-block-format` | MIT |
| go-datastore | `github.com/ipfs/go-datastore` | MIT |
| go-ds-leveldb | `github.com/ipfs/go-ds-leveldb` | MIT |
| go-ipld-format | `github.com/ipfs/go-ipld-format` | MIT |
| go-codec-dagpb | `github.com/ipld/go-codec-dagpb` | Apache-2.0 OR MIT |
| go-ipld-prime | `github.com/ipld/go-ipld-prime` | MIT |
| go-multiaddr | `github.com/multiformats/go-multiaddr` | MIT |
| go-multihash | `github.com/multiformats/go-multihash` | MIT |
| go-fuse | `github.com/hanwen/go-fuse/v2` | BSD-3-Clause |
| prometheus/client_golang | `github.com/prometheus/client_golang` | Apache-2.0 |
| BurntSushi/toml | `github.com/BurntSushi/toml` | MIT |
| fxamacker/cbor | `github.com/fxamacker/cbor/v2` | MIT |
| google/uuid | `github.com/google/uuid` | BSD-3-Clause |
| protobuf | `google.golang.org/protobuf` | BSD-3-Clause |

## Indirect dependencies

The transitive dependencies pulled in through the libraries above are
pinned in `go.sum` and are all distributed under the same family of
permissive licences (MIT, BSD-3-Clause, Apache-2.0). Their licence
texts are not mirrored here; they are available through each module's
source repository and in the Go module cache under
`$(go env GOMODCACHE)`.

## How to refresh this directory

From the project root:

```
go mod download
scripts/refresh-third-party-licenses.sh
```

(The script is a thin wrapper that copies `LICENSE*` / `COPYING*`
files out of the module cache into this directory; regenerate it when
the dependency set changes.)
