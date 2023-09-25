package index_test

import (
	"strings"
	"testing"

	"github.com/ttab/elephant-index/index"
)

type expectation struct {
	code     string
	name     string
	language string
	analyzer string
}

var params = []expectation{
	{code: "sv-SE", name: "sv-se", language: "sv", analyzer: "swedish"},
	{code: "sv-FI", name: "sv-fi", language: "sv", analyzer: "swedish"},
	{code: "pt-BR", name: "pt-br", language: "pt", analyzer: "brazilian"},
	{code: "pt-PT", name: "pt-pt", language: "pt", analyzer: "portuguese"},
	{code: "en-US", name: "en-us", language: "en", analyzer: "english"},
	{code: "en-NZ", name: "en-nz", language: "en", analyzer: "english"},
	{code: "az", name: "az", language: "ru", analyzer: "russian"},
	{code: "jp", name: "standard", language: "", analyzer: ""},
}

func TestGetLanguageSetting(t *testing.T) {
	for i := range params {
		s, _ := index.GetIndexSettings(params[i].code)
		if s.Name != params[i].name {
			t.Fatalf("expected Name: %q, got %q", params[i].name, s.Name)
		}
		if s.Language != params[i].language {
			t.Fatalf("expected Language: %q, got %q", params[i].language, s.Language)
		}
		if !strings.Contains(s.Settings, params[i].analyzer) {
			t.Fatalf("expected settings default Analyzer: %q, got %q", params[i].analyzer, s.Settings)
		}
	}

}
