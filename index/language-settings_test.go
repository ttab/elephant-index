package index_test

import (
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
	{code: "sv-se", name: "sv-se", language: "sv", analyzer: "swedish"},
	{code: "sv-FI", name: "sv-fi", language: "sv", analyzer: "swedish"},
	{code: "sv-fi", name: "sv-fi", language: "sv", analyzer: "swedish"},
	{code: "pt-BR", name: "pt-br", language: "pt", analyzer: "brazilian"},
	{code: "pt-br", name: "pt-br", language: "pt", analyzer: "brazilian"},
	{code: "pt-PT", name: "pt-pt", language: "pt", analyzer: "portuguese"},
	{code: "pt-pt", name: "pt-pt", language: "pt", analyzer: "portuguese"},
	{code: "ja-JP", name: "ja-jp", language: "ja", analyzer: "standard"},
	{code: "ja-jp", name: "ja-jp", language: "ja", analyzer: "standard"},
}

func TestGetLanguageSetting(t *testing.T) {
	for _, param := range params {
		s, _ := index.GetLanguageConfig(param.code)

		if s.Name != param.name {
			t.Fatalf("%s: expected Name: %q, got %q", param.code, param.name, s.Name)
		}

		if s.Language != param.language {
			t.Fatalf("%s: expected Language: %q, got %q", param.code, param.language, s.Language)
		}

		if s.Settings.Settings.Analysis.Analyzer.Default.Type != param.analyzer {
			t.Fatalf("%s: expected settings default Analyzer: %q, got %q", param.code,
				param.analyzer,
				s.Settings.Settings.Analysis.Analyzer.Default.Type)
		}
	}
}
