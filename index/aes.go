package index

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

func encryptPassword(password string, key [32]byte) (string, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())

	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	encrypted := gcm.Seal(nonce, nonce, []byte(password), nil)

	versioned := "v1." + base64.RawURLEncoding.EncodeToString(encrypted)

	return versioned, nil
}

func decryptPassword(
	encryptedPassword string,
	key [32]byte,
) (string, uint, error) {
	version, encoded, ok := strings.Cut(encryptedPassword, ".")
	if !ok {
		return "", 0, errors.New("invalid format")
	}

	// Just getting the version information in place to support future
	// upgrades. Future implementations could f.ex. accept a keyfunc instead
	// of a concrete key.
	if version != "v1" {
		return "", 0, fmt.Errorf("unsupported version %q", version)
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", 0, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", 0, fmt.Errorf("create GCM: %w", err)
	}

	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", 0, fmt.Errorf("invalid encrypted payload: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(sealed) < nonceSize {
		return "", 0, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := sealed[:nonceSize], sealed[nonceSize:]

	password, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", 0, fmt.Errorf("invalid payload: %w", err)
	}

	return string(password), 1, nil
}
