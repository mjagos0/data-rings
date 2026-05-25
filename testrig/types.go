package testrig

type NodeState struct {
	ID			string		`json:"id"`
	Addr			string		`json:"addr"`
	Successor		NodeAddr	`json:"successor"`
	SuccessorList		[]NodeAddr	`json:"successor_list"`
	Predecessor		*NodeAddr	`json:"predecessor"`
	Fingers			[]NodeAddr	`json:"fingers"`
	BlockCount		int		`json:"block_count"`
	RecordCount		int		`json:"record_count"`
	StorageUsedBytes	int64		`json:"storage_used_bytes"`
	StorageMaxBytes		int64		`json:"storage_max_bytes"`

	RingID			string	`json:"ring_id"`
	RingBlockCount		int	`json:"ring_block_count"`
	RingStorageUsedBytes	int64	`json:"ring_storage_used_bytes"`
	RingStorageMaxBytes	int64	`json:"ring_storage_max_bytes"`
	RingNetworkRootCount	int	`json:"ring_network_root_count"`
}

type NodeAddr struct {
	ID	string	`json:"ID"`
	Addr	string	`json:"Addr"`
}

type PrivateRingEntry struct {
	GroupID		string			`json:"group_id"`
	Name		string			`json:"name"`
	ListenAddr	string			`json:"listen_addr"`
	Node		NodeState		`json:"node"`
	VerifiedPeers	[]string		`json:"verified_peers"`
	ConnectionPool	map[string]string	`json:"connection_pool"`
}
