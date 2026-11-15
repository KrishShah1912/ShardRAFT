package transport

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/KrishShah1912/shardraft/internal/raft"
)

// Peer identifies a remote Raft node and its network address.
type Peer struct {
	ID      raft.NodeID
	Address string
}

// ParsePeers parses:
// 2=127.0.0.1:8082,3=127.0.0.1:8083
func ParsePeers(value string) ([]Peer, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	var peers []Peer
	seen := make(map[raft.NodeID]bool)

	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return nil, errors.New("peer must use ID=host:port")
		}

		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid peer ID %q", parts[0])
		}

		peerID := raft.NodeID(id)
		if seen[peerID] {
			return nil, fmt.Errorf("duplicate peer ID %d", peerID)
		}

		address := strings.TrimSpace(parts[1])
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return nil, fmt.Errorf("invalid peer address %q", address)
		}

		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("invalid peer port %q", port)
		}

		seen[peerID] = true
		peers = append(peers, Peer{
			ID:      peerID,
			Address: address,
		})
	}

	return peers, nil
}
