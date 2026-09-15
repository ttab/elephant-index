package main

import (
	"testing"
)

// TestParseDefaultCluster covers the flag that decides whether a fresh
// installation gets a default cluster and index set at all. It is here because
// the path had no coverage: the previous inline version discarded the parsed
// URL into a shadowed variable, so --opensearch-endpoint was silently ignored
// and nothing said so.
func TestParseDefaultCluster(t *testing.T) {
	t.Parallel()

	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}

	t.Run("empty endpoint yields no cluster", func(t *testing.T) {
		t.Parallel()

		osURL, auth, err := parseDefaultCluster("", false, key)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		if osURL != nil {
			t.Fatalf("got %q, want no URL", osURL)
		}

		if auth.IAM {
			t.Fatal("IAM must follow the flag, not the endpoint")
		}
	})

	t.Run("endpoint is returned", func(t *testing.T) {
		t.Parallel()

		osURL, _, err := parseDefaultCluster(
			"https://os.example.com:9200", false, key)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		// The regression this guards: a nil URL here means RunIndex
		// gets a nil DefaultCluster and never creates an index set.
		if osURL == nil {
			t.Fatal("the parsed endpoint must be returned, got nil")
		}

		if got := osURL.String(); got != "https://os.example.com:9200" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("managed opensearch selects IAM", func(t *testing.T) {
		t.Parallel()

		_, auth, err := parseDefaultCluster(
			"https://os.example.com", true, key)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		if !auth.IAM {
			t.Fatal("want IAM signing")
		}
	})

	t.Run("credentials move out of the URL", func(t *testing.T) {
		t.Parallel()

		osURL, auth, err := parseDefaultCluster(
			"https://user:s3cret@os.example.com", true, key)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		// The URL is stored on the cluster row, so it must not carry
		// the password.
		if got := osURL.String(); got != "https://os.example.com" {
			t.Fatalf("credentials left in the URL: %q", got)
		}

		if auth.Username != "user" {
			t.Fatalf("got username %q", auth.Username)
		}

		if auth.Password == "s3cret" {
			t.Fatal("the password must be stored encrypted")
		}

		pw, err := auth.GetPassword(key)
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}

		if pw != "s3cret" {
			t.Fatalf("got %q back", pw)
		}

		// Explicit credentials are a choice of basic auth over IAM,
		// even with --managed-opensearch set.
		if auth.IAM {
			t.Fatal("explicit credentials must turn IAM off")
		}
	})

	t.Run("an unparseable endpoint is an error", func(t *testing.T) {
		t.Parallel()

		_, _, err := parseDefaultCluster(
			"https://os.example.com/%zz", false, key)
		if err == nil {
			t.Fatal("want an error")
		}
	})
}
