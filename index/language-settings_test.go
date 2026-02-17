package index_test

import (
	"testing"

	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine/test"
)

type expectation struct {
	code           string
	defaultCode    string
	name           string
	language       string
	analyzer       string
	defaultRegions map[string]string
	substitutions  map[string]string
}

var params = []expectation{
	{code: "sv-SE", name: "core_article-sv-se", language: "sv", analyzer: "swedish"},
	{code: "sv-se", name: "core_article-sv-se", language: "sv", analyzer: "swedish"},
	{code: "sv-FI", name: "core_article-sv-fi", language: "sv", analyzer: "swedish"},
	{code: "sv-fi", name: "core_article-sv-fi", language: "sv", analyzer: "swedish"},
	{code: "sv", name: "core_article-sv-unspecified", language: "sv", analyzer: "swedish"},
	{
		code: "sv",
		name: "core_article-sv-se", language: "sv", analyzer: "swedish",
		defaultRegions: map[string]string{
			"sv": "SE",
		},
	},
	{
		code: "se",
		name: "core_article-sv-se", language: "sv", analyzer: "swedish",
		substitutions: map[string]string{
			"se": "sv",
		},
		defaultRegions: map[string]string{
			"sv": "se",
		},
	},
	{code: "pt-BR", name: "core_article-pt-br", language: "pt", analyzer: "brazilian"},
	{code: "pt-br", name: "core_article-pt-br", language: "pt", analyzer: "brazilian"},
	{code: "pt-PT", name: "core_article-pt-pt", language: "pt", analyzer: "portuguese"},
	{code: "pt-pt", name: "core_article-pt-pt", language: "pt", analyzer: "portuguese"},
	{code: "pt", name: "core_article-pt-unspecified", language: "pt", analyzer: "portuguese"},
	{code: "ja-JP", name: "core_article-ja-jp", language: "ja", analyzer: "standard"},
	{code: "ja-jp", name: "core_article-ja-jp", language: "ja", analyzer: "standard"},
	{code: "ja", name: "core_article-ja-unspecified", language: "ja", analyzer: "standard"},
	{code: "", defaultCode: "sv-SE", name: "core_article-sv-se", language: "sv", analyzer: "swedish"},
	{code: "", defaultCode: "fi-fi", name: "core_article-fi-fi", language: "fi", analyzer: "finnish"},
}

func TestGetLanguageSetting(t *testing.T) {
	for _, param := range params {
		res := index.NewLanguageResolver(index.LanguageOptions{
			DefaultLanguage: param.defaultCode,
			Substitutions:   param.substitutions,
			DefaultRegions:  param.defaultRegions,
		})

		lang, err := res.GetLanguageInfo(param.code)
		test.Must(t, err, "get language info")

		s := index.GetIndexConfig(lang)
		idx := index.NewIndexName(
			index.IndexTypeDocuments,
			"happy-hog", "core/article", lang)

		if idx.Language != param.name {
			t.Fatalf("%s: expected Name: %q, got %q", param.code, param.name, idx.Language)
		}

		if lang.Language != param.language {
			t.Fatalf("%s: expected Language: %q, got %q", param.code, param.language, s.Language)
		}

		analysis := s.Settings.Settings.Analysis

		if analysis.Analyzer == nil || analysis.Analyzer["default"].Type != param.analyzer {
			t.Fatalf("%s: expected settings default Analyzer: %q, got %q",
				param.code,
				param.analyzer,
				analysis.Analyzer["default"].Type)
		}
	}

	t.Run("variant type", func(t *testing.T) {
		res := index.NewLanguageResolver(index.LanguageOptions{})

		lang, err := res.GetLanguageInfo("sv-SE")
		test.Must(t, err, "get language info")

		idx := index.NewIndexName(
			index.IndexTypeDocuments,
			"happy-hog", "core/article+template", lang)

		test.Equal(t, "core_article--template-sv-se", idx.Language,
			"variant type language name")
		test.Equal(t, "documents-happy-hog-core_article--template-sv-se", idx.Full,
			"variant type full name")
	})
}
