package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

const (
	defaultConfig		= "test-fleet.toml"
	defaultGoVersion	= "1.25.7"
	defaultSSHUser		= "admin"
)

type Config struct {
	PEMKey		string		`toml:"pem_key"`
	GoVersion	string		`toml:"go_version"`
	AWSKeyPair	string		`toml:"aws_key_pair"`
	Monitoring	Monitoring	`toml:"monitoring"`
	Groups		[]Group		`toml:"groups"`
	Instances	[]Instance	`toml:"instances"`
}

type Monitoring struct {
	Enabled		bool	`toml:"enabled"`
	LokiURL		string	`toml:"loki_url"`
	MetricsPort	string	`toml:"metrics_port"`
}

type Group struct {
	Name		string	`toml:"name"`
	PrivateKey	string	`toml:"private_key"`
}

type Instance struct {
	Name			string		`toml:"name"`
	AWSName			string		`toml:"aws_name"`
	Region			string		`toml:"region"`
	IPv4			string		`toml:"ipv4"`
	SSHUser			string		`toml:"ssh_user"`
	Initialized		bool		`toml:"initialized"`
	IsBootstrap		bool		`toml:"is_bootstrap"`
	StaticIP		bool		`toml:"static_ip"`
	Groups			[]string	`toml:"groups"`
	FounderOf		[]string	`toml:"founder_of"`
	PeerIdentityRecord	string		`toml:"peer_identity_record"`

	Local	bool	`toml:"local"`
	APIPort	int	`toml:"api_port"`
	LogFile	string	`toml:"log_file"`
	DataDir	string	`toml:"data_dir"`
}

func (inst *Instance) isLocal() bool	{ return inst.Local }

func (inst *Instance) awsName() string {
	if inst.AWSName != "" {
		return inst.AWSName
	}
	return inst.Name
}

func loadConfig(path string) (*Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	for i := range cfg.Instances {
		if cfg.Instances[i].SSHUser == "" {
			cfg.Instances[i].SSHUser = defaultSSHUser
		}
	}
	if cfg.GoVersion == "" {
		cfg.GoVersion = defaultGoVersion
	}
	return &cfg, nil
}

func saveConfig(path string, cfg *Config) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return toml.NewEncoder(f).Encode(cfg)
}

func (cfg *Config) findInstance(name string) (*Instance, int, error) {
	for i := range cfg.Instances {
		if cfg.Instances[i].Name == name || cfg.Instances[i].IPv4 == name {
			return &cfg.Instances[i], i, nil
		}
	}
	return nil, -1, fmt.Errorf("instance %q not found in config", name)
}

func (cfg *Config) findGroup(name string) (*Group, error) {
	for i := range cfg.Groups {
		if cfg.Groups[i].Name == name {
			return &cfg.Groups[i], nil
		}
	}
	return nil, fmt.Errorf("group %q not found in config", name)
}

func (cfg *Config) bootstrapInstance() *Instance {
	for i := range cfg.Instances {
		if cfg.Instances[i].IsBootstrap {
			return &cfg.Instances[i]
		}
	}
	if len(cfg.Instances) > 0 {
		return &cfg.Instances[0]
	}
	return nil
}

func (cfg *Config) resolveInstances(names []string) ([]*Instance, error) {
	if len(names) == 0 {
		result := make([]*Instance, len(cfg.Instances))
		for i := range cfg.Instances {
			result[i] = &cfg.Instances[i]
		}
		return result, nil
	}
	var result []*Instance
	for _, name := range names {
		inst, _, err := cfg.findInstance(name)
		if err != nil {
			return nil, err
		}
		result = append(result, inst)
	}
	return result, nil
}

const pemKeysDir = ".aws-keys"

func resolvePEMFile(cfg *Config) (string, error) {
	if cfg.AWSKeyPair == "" {
		return "", fmt.Errorf("aws_key_pair is empty in fleet config")
	}
	src := filepath.Join(findProjectRoot(), pemKeysDir, cfg.AWSKeyPair+".pem")
	data, err := os.ReadFile(src)
	if err != nil {
		return "", fmt.Errorf("PEM key not found at %s: %w (persist the private key for AWS key-pair %q manually)", src, err, cfg.AWSKeyPair)
	}
	f, err := os.CreateTemp("", "drings-pem-"+cfg.AWSKeyPair+"-*.pem")
	if err != nil {
		return "", fmt.Errorf("stage PEM: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("write staged PEM: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", fmt.Errorf("chmod staged PEM: %w", err)
	}
	f.Close()
	return f.Name(), nil
}
