package transport

import (
	"testing"
)

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers(
		"2=127.0.0.1:8082,3=127.0.0.1:8083",
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	if peers[0].ID != 2 || peers[0].Address != "127.0.0.1:8082" {
		t.Fatalf("unexpected first peer: %+v", peers[0])
	}

	if peers[1].ID != 3 || peers[1].Address != "127.0.0.1:8083" {
		t.Fatalf("unexpected second peer: %+v", peers[1])
	}
}

func TestParsePeersRejectsInvalid(t *testing.T) {
	cases := []string{
		"abc",
		"0=127.0.0.1:8080",
		"x=127.0.0.1:8080",
		"2=invalid",
		"2=127.0.0.1:0",
		"2=127.0.0.1:99999",
		"2=127.0.0.1:8082,2=127.0.0.1:8083",
		"2=",
	}

	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			if _, err := ParsePeers(value); err == nil {
				t.Fatalf("expected error for %q", value)
			}
		})
	}
}

func TestParsePeersEmpty(t *testing.T) {
	peers, err := ParsePeers("")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected no peers, got %d", len(peers))
	}
}
