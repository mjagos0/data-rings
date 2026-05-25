package dht

import (
	"encoding/json"
	"fmt"
	"os"
)

type GroupConfig struct {
	GroupPrivKeyHex	string	`json:"group_priv_key"`

	ListenAddr	string	`json:"listen_addr"`

	Name	string	`json:"name,omitempty"`

	StorageMaxBytes	int64	`json:"storage_max_bytes,omitempty"`
}

type GroupsFile struct {
	Groups []GroupConfig `json:"groups"`
}

func LoadGroupsFile(path string) (*GroupsFile, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &GroupsFile{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read groups file %s: %w", path, err)
	}
	var f GroupsFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse groups file %s: %w", path, err)
	}
	return &f, nil
}

func SaveGroupsFile(path string, f *GroupsFile) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal groups file: %w", err)
	}
	return os.WriteFile(path, data, 0600)
}

func (f *GroupsFile) AddGroup(cfg GroupConfig) {

	newGrp, err := GroupIdentityFromHex(cfg.GroupPrivKeyHex)
	if err != nil {

		f.Groups = append(f.Groups, cfg)
		return
	}

	for i, g := range f.Groups {
		existing, err := GroupIdentityFromHex(g.GroupPrivKeyHex)
		if err != nil {
			continue
		}
		if existing.GroupID == newGrp.GroupID {
			f.Groups[i] = cfg
			return
		}
	}
	f.Groups = append(f.Groups, cfg)
}

func (f *GroupsFile) RemoveGroup(groupIDHex string) bool {
	before := len(f.Groups)
	out := f.Groups[:0]
	for _, g := range f.Groups {
		grp, err := GroupIdentityFromHex(g.GroupPrivKeyHex)
		if err != nil {
			out = append(out, g)
			continue
		}
		if grp.GroupID.String() != groupIDHex {
			out = append(out, g)
		}
	}
	f.Groups = out
	return len(f.Groups) < before
}
