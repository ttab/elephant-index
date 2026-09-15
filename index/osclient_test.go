package index_test

import (
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine/test"
)

func TestClusterAuthPassword(t *testing.T) {
	var key, keyB [32]byte

	_, err := io.ReadFull(rand.Reader, key[:])
	test.Mustf(t, err, "create key")

	_, err = io.ReadFull(rand.Reader, keyB[:])
	test.Mustf(t, err, "create key B")

	var auth index.ClusterAuth

	pass := "supersafesecret"

	err = auth.SetPassword(pass, key)
	test.Mustf(t, err, "encrypt password")

	if strings.Contains(auth.Password, pass) {
		t.Fatal("encrypted password cannot contain the password")
	}

	dec, err := auth.GetPassword(key)
	test.Mustf(t, err, "decrypt stored password")

	test.Equalf(t, pass, dec, "the decrypted password must match the original password")

	_, err = auth.GetPassword(keyB)
	test.MustNotf(t, err, "must not decrypt stored password with another key")
}
