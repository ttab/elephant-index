package index_test

import (
	"fmt"
	"sync"
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
		test.Mustf(t, err, "get language info")

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
		test.Mustf(t, err, "get language info")

		idx := index.NewIndexName(
			index.IndexTypeDocuments,
			"happy-hog", "core/article#template", lang)

		test.Equalf(t, "core_article--template-sv-se", idx.Language,
			"variant type language name")
		test.Equalf(t, "documents-happy-hog-core_article--template-sv-se", idx.Full,
			"variant type full name")
	})
}

// TestLanguageResolverConcurrentUse is a regression test for ELE-1563: the
// resolver's memo used to be an unguarded map, and the coordinator hands one
// resolver to every indexer, so a re-index cutover crashed the process with
// "fatal error: concurrent map writes". Run it with -race.
func TestLanguageResolverConcurrentUse(t *testing.T) {
	codes := []string{
		"sv-se", "en-gb", "en-us", "it-it", "da-dk", "nb-no", "de-de",
		"es-es", "fi-fi", "fr-fr", "nl-nl", "pt-br", "pt-pt", "ru-ru",
		"ja-jp", "th-th", "tr-tr", "el-gr", "cs-cz", "hu-hu",
	}

	res := index.NewLanguageResolver(index.LanguageOptions{
		DefaultLanguage: "sv-se",
	})

	const workers = 8

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
	)

	start.Add(1)
	done.Add(workers)

	errs := make([]error, workers)

	for w := range workers {
		go func() {
			defer done.Done()

			// Line all the goroutines up so they hit the cold cache
			// at the same time.
			start.Wait()

			// Each worker walks the whole list from its own offset:
			// the first lap collides on cold codes, and the laps
			// that follow race reads against the writes of the
			// workers still behind.
			for lap := range 50 {
				for i := range codes {
					code := codes[(w+lap+i)%len(codes)]

					info, err := res.GetLanguageInfo(code)
					if err != nil {
						errs[w] = fmt.Errorf(
							"resolve %q: %w", code, err)

						return
					}

					if info.Code != code {
						errs[w] = fmt.Errorf(
							"resolve %q: got code %q",
							code, info.Code)

						return
					}
				}
			}
		}()
	}

	start.Done()
	done.Wait()

	for w, err := range errs {
		test.Mustf(t, err, "worker %d", w)
	}
}
