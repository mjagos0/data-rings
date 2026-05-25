package dht

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	authNonceSize	= 16
	authHMACSize	= 32
)

func computeAuthHMAC(psk, nonce []byte, idA, idB NodeID) []byte {
	mac := hmac.New(sha256.New, psk)
	mac.Write(nonce)
	mac.Write(idA[:])
	mac.Write(idB[:])
	return mac.Sum(nil)
}

func PerformClientAuth(conn net.Conn, psk []byte, selfID, peerID NodeID) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	var nb [authNonceSize]byte
	if _, err := io.ReadFull(conn, nb[:]); err != nil {
		return fmt.Errorf("auth client: read server nonce: %w", err)
	}

	var na [authNonceSize]byte
	if _, err := io.ReadFull(rand.Reader, na[:]); err != nil {
		return fmt.Errorf("auth client: generate nonce: %w", err)
	}
	hmacA := computeAuthHMAC(psk, nb[:], selfID, peerID)

	var msg [20 + authHMACSize + authNonceSize]byte
	copy(msg[:20], selfID[:])
	copy(msg[20:20+authHMACSize], hmacA)
	copy(msg[20+authHMACSize:], na[:])
	if _, err := conn.Write(msg[:]); err != nil {
		return fmt.Errorf("auth client: send auth message: %w", err)
	}

	var hmacB [authHMACSize]byte
	if _, err := io.ReadFull(conn, hmacB[:]); err != nil {
		return fmt.Errorf("auth client: read server HMAC: %w", err)
	}
	expectedB := computeAuthHMAC(psk, na[:], peerID, selfID)
	if !hmac.Equal(hmacB[:], expectedB) {
		return fmt.Errorf("auth client: server HMAC verification failed — wrong PSK or tampered message")
	}
	return nil
}

func PerformServerAuth(conn net.Conn, psk []byte, selfID NodeID) (NodeID, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	var nb [authNonceSize]byte
	if _, err := io.ReadFull(rand.Reader, nb[:]); err != nil {
		return NodeID{}, fmt.Errorf("auth server: generate nonce: %w", err)
	}
	if _, err := conn.Write(nb[:]); err != nil {
		return NodeID{}, fmt.Errorf("auth server: send nonce: %w", err)
	}

	var clientMsg [20 + authHMACSize + authNonceSize]byte
	if _, err := io.ReadFull(conn, clientMsg[:]); err != nil {
		return NodeID{}, fmt.Errorf("auth server: read client message: %w", err)
	}
	var idA NodeID
	copy(idA[:], clientMsg[:20])
	clientHMAC := clientMsg[20 : 20+authHMACSize]
	na := clientMsg[20+authHMACSize:]

	expectedA := computeAuthHMAC(psk, nb[:], idA, selfID)
	if !hmac.Equal(clientHMAC, expectedA) {
		return NodeID{}, fmt.Errorf("auth server: client HMAC verification failed — wrong PSK or tampered message")
	}

	hmacB := computeAuthHMAC(psk, na, selfID, idA)
	if _, err := conn.Write(hmacB); err != nil {
		return NodeID{}, fmt.Errorf("auth server: send HMAC: %w", err)
	}

	return idA, nil
}
