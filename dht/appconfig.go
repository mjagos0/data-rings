package dht

import (
	"os"

	"github.com/BurntSushi/toml"
)

var DefaultBootstrapPeers = []string{
	"/ip4/18.184.76.53/tcp/7000",
	"/ip4/18.194.176.202/tcp/7000",
}

type AppConfig struct {
	BootstrapPeers	[]string	`toml:"bootstrap_peers"`

	ListenAddr	string	`toml:"listen_addr"`

	Replication	int	`toml:"replication"`

	StorageMax	string	`toml:"storage_max"`

	GCInterval	string	`toml:"gc_interval"`

	MountPath	string	`toml:"mount_path"`
}

func LoadAppConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &AppConfig{}, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg AppConfig
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
