To learn about this research project, refer to chapters 5 to 7 of: [thesis.pdf](thesis.pdf).

## Requirements

Operating systems:
| OS      | Runtime  | FUSE   |
|---------|----------|--------|
| Linux   | yes      | yes    |
| macOS   | yes      | no     |
| Windows | no       | no     |

Build:
- Go 1.25 or newer
- `make`

For FUSE:
- Linux with the `fuse` kernel module loaded
- `fusermount` available at `/bin/fusermount`

## Install (Debian/Ubuntu)

Install Go:

```
curl -fsSL https://go.dev/dl/go1.25.7.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
export PATH=$PATH:/usr/local/go/bin
go version
```

Install dependencies:
```
sudo apt install -y make fuse build-essential
```

Compile:
```
make build
```

## Run

With FUSE:

```
sudo ./drings setup # or manually (see below)
./drings-daemon
```

Mounts to `/mnt/datarings` by default. The creation of the `/mnt/datarings` directory may require sudo, and must carry permissions of the user running `./drings-daemon`. Either create the directory and assign correct permissions, or use a convenience `setup` script command.

Mount to a custom directory:
```
./drings-daemon --mount-path /some/dir
```

Without FUSE:
```
./drings-daemon --mount=false
```

### Daemon configuration

```
./drings-daemon --help
```

The first time daemon runs, it creates `~/.datarings/config.toml` to persist settings.

### Public DHT Bootstrap

Daemon connects to public DHT automatically. The application includes address of a bootstrap peer. This peer may not be running and a new public DHT will be created. Other peers can join the network by referencing your address with the `./drings-daemon --bootstrap flag`, or modifying `~/.datarings/config.toml`.

### Listening ports

The daemon opens TCP listeners for the following services:

| Listener      | Default                  | Override                                                                |
|---------------|--------------------------|-------------------------------------------------------------------------|
| Public DHT    | `/ip4/0.0.0.0/tcp/7000`  | `--dht-addr=<multiaddr>` or `listen_addr` in `~/.datarings/config.toml` |
| HTTP API      | `:7423` (all interfaces) | `--api-addr=<host:port>`                                                |
| Metrics       | disabled                 | `--metrics-addr=<host:port>` to enable                                  |
| Private rings | OS-assigned              | `--listen-addr=/ip4/0.0.0.0/tcp/<port>` on `./drings ring join`         |

## Using the application

Daemon must be running. Use user-facing command-line tool to interact with the application:

```
./drings --help
```

### Examples

Serialize a file system into local data store:

```
./drings add <path>
```

The file system should become interactable through FUSE, or with `./drings list` command.

Join a private group:

```
./drings ring join <key> <alias> 
```

Or create a new one:
```
./drings ring create <alias>
./drings ring join <alias>
```

If the bootstrap peer is running, you can use pre-existing group for testing:
```
./drings ring join 256569bb655f7f1f82e8087664f9a6702c3c82a50719989b5d29b523d66bbd1e61a3df842b977c14e63135b976d8d2b258806d28155f9312c8ac21a80299e97a test-ring
```

To upload the file system to the group, use the returned CID from `./drings add <path>` command and execute:

```
./drings pub test-ring <CID>
```

The file system will become available to peers in the private ring, and it can be fetched as

```
./drings get test-ring <cid>
```

Alternatively, fetch a pre-existing CID that the bootstrap peer is storing:

```
$ ./drings get test-ring QmU5Axiwqit7EfnkxUJb4g1ejweiBD7WuLwzAmcwsfwN8n cats
fetched
cid:  QmU5Axiwqit7EfnkxUJb4g1ejweiBD7WuLwzAmcwsfwN8n
name: cats

$ tree -L 2 /mnt/datarings/cats
/mnt/datarings/cats
├── dorota
│   ├── PXL_20260403_105125253.jpg
│   ├── PXL_20260404_143047571.jpg
│   └── PXL_20260404_143111741.jpg
├── modrák
│   ├── PXL_20260405_065033892.PORTRAIT.jpg
│   ├── PXL_20260405_065138794.jpg
│   └── PXL_20260405_065145481.PORTRAIT.jpg
└── pets.txt

3 directories, 7 files
```

| Dorota | Modrák |
|:---:|:---:|
| <img src="cat/dorota/PXL_20260403_105125253.jpg" width="320"> | <img src="cat/modr%C3%A1k/PXL_20260405_065033892.PORTRAIT.jpg" width="320"> |

## Deploy your own cluster

Configure your instances in `./test-fleet.toml`, and redirect the application to a bootstrap peer in `./cmd/drings-daemon/config.toml`. This file is copied to `~/.datarings/config.toml` when instances run the daemon for the first time.

```
./drings-deploy --help
```
