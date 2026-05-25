package dht

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Vault struct {
	Entries	[]VaultEntry	`json:"entries"`
	path	string
}

type VaultEntry struct {
	Alias	string	`json:"alias"`
	Key	string	`json:"key"`
}

func LoadVault(path string) (*Vault, error) {
	v := &Vault{path: path}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return v, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return nil, fmt.Errorf("parse vault %s: %w", path, err)
	}
	v.path = path
	return v, nil
}

func (v *Vault) save() error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(v.path, data, 0600)
}

func (v *Vault) Add(alias, keyHex string) error {
	if alias == "" {
		return fmt.Errorf("alias must not be empty")
	}

	if _, err := GroupIdentityFromHex(keyHex); err != nil {
		return fmt.Errorf("invalid group key: %w", err)
	}
	for i, e := range v.Entries {
		if e.Alias == alias {
			v.Entries[i].Key = keyHex
			return v.save()
		}
	}
	v.Entries = append(v.Entries, VaultEntry{Alias: alias, Key: keyHex})
	return v.save()
}

func (v *Vault) Remove(alias string) (bool, error) {
	before := len(v.Entries)
	out := v.Entries[:0]
	for _, e := range v.Entries {
		if e.Alias != alias {
			out = append(out, e)
		}
	}
	v.Entries = out
	if len(v.Entries) == before {
		return false, nil
	}
	return true, v.save()
}

func (v *Vault) Lookup(alias string) (*GroupIdentity, error) {
	keyHex, err := v.LookupKey(alias)
	if err != nil {
		return nil, err
	}
	return GroupIdentityFromHex(keyHex)
}

func (v *Vault) LookupKey(alias string) (string, error) {

	for _, e := range v.Entries {
		if e.Alias == alias {
			return e.Key, nil
		}
	}

	var matched *VaultEntry
	for i := range v.Entries {
		if strings.HasPrefix(v.Entries[i].Alias, alias) {
			if matched != nil {
				return "", fmt.Errorf("alias %q is ambiguous", alias)
			}
			matched = &v.Entries[i]
		}
	}
	if matched != nil {
		return matched.Key, nil
	}
	return "", fmt.Errorf("alias %q not found in vault (use 'drings key list' to see registered keys)", alias)
}

type VaultInfo struct {
	Alias	string
	GroupID	string
}

func (v *Vault) List() []VaultInfo {
	out := make([]VaultInfo, 0, len(v.Entries))
	for _, e := range v.Entries {
		grp, err := GroupIdentityFromHex(e.Key)
		if err != nil {

			out = append(out, VaultInfo{Alias: e.Alias, GroupID: "(invalid key)"})
			continue
		}
		out = append(out, VaultInfo{Alias: e.Alias, GroupID: grp.GroupID.String()})
	}
	return out
}

func ResolveGroupKey(v *Vault, s string) (keyHex string, alias string, err error) {

	if _, parseErr := GroupIdentityFromHex(s); parseErr == nil {
		return s, "", nil
	}

	keyHex, err = v.LookupKey(s)
	if err != nil {
		return "", "", err
	}
	return keyHex, s, nil
}
