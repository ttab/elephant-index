package index

import (
	"fmt"
	"strings"

	"github.com/ttab/langos"
)

type LanguageConfig struct {
	NameSuffix string
	Language   string
	Settings   OpensearchSettings
}

type OpensearchSettings struct {
	Settings struct {
		Analysis OpensearchAnalysis `json:"analysis"`
	} `json:"settings"`
}

type OpensearchAnalysis struct {
	Analyzer   map[string]OpensearchAnalyzer   `json:"analyzer,omitempty"`
	Normalizer map[string]OpensearchNormaliser `json:"normalizer,omitempty"`
}

func (osa *OpensearchAnalysis) SetAnalyzer(name string, v OpensearchAnalyzer) {
	if osa.Analyzer == nil {
		osa.Analyzer = make(map[string]OpensearchAnalyzer)
	}

	osa.Analyzer[name] = v
}

func (osa *OpensearchAnalysis) SetNormalizer(name string, v OpensearchNormaliser) {
	if osa.Normalizer == nil {
		osa.Normalizer = make(map[string]OpensearchNormaliser)
	}

	osa.Normalizer[name] = v
}

type OpensearchAnalyzer struct {
	Type string `json:"type"`
}

type OpensearchNormaliser struct {
	Type   string   `json:"type"`
	Filter []string `json:"filter"`
}

func GetLanguageConfig(code string, defaultLanguage string) (LanguageConfig, error) {
	if code == "" {
		code = defaultLanguage
	}

	info, err := langos.GetLanguage(code)
	if err != nil {
		return LanguageConfig{}, fmt.Errorf("get language: %w", err)
	}

	code = strings.ToLower(info.Code)
	lang := strings.ToLower(info.Language)

	region := "unspecified"
	if info.HasRegion {
		region = strings.ToLower(info.Region)
	}

	analyzer := "standard"

	for _, ls := range languages {
		if ls.Code == code {
			analyzer = ls.Analyzer

			break
		}

		if ls.Language == lang {
			analyzer = ls.Analyzer
		}
	}

	var s OpensearchSettings

	s.Settings.Analysis.SetAnalyzer("default", OpensearchAnalyzer{
		Type: analyzer,
	})

	return LanguageConfig{
		NameSuffix: fmt.Sprintf("%s-%s", lang, region),
		Language:   lang,
		Settings:   s,
	}, nil
}

type Language struct {
	Code     string
	Language string
	Analyzer string
}

// Order matters; the matching algorithm will pick the first exact Code match,
// or failing that, the last Language match.
var languages = []Language{
	// Portuguese is a special case since it can be either european or brazilian
	// depending on the region. This line makes european the default if no
	// region was given.
	{Code: "pt", Language: "pt", Analyzer: "portuguese"},

	{Code: "ar-eg", Language: "ar", Analyzer: "arabic"},
	{Code: "hy-am", Language: "hy", Analyzer: "armenian"},
	{Code: "eu-es", Language: "eu", Analyzer: "basque"},
	{Code: "bn-bd", Language: "bn", Analyzer: "bengali"},
	{Code: "pt-br", Language: "pt", Analyzer: "brazilian"},
	{Code: "bg-bg", Language: "bg", Analyzer: "bulgarian"},
	{Code: "ca-es", Language: "ca", Analyzer: "catalan"},
	{Code: "cs-cz", Language: "cs", Analyzer: "czech"},
	{Code: "da-dk", Language: "da", Analyzer: "danish"},
	{Code: "nl-nl", Language: "nl", Analyzer: "dutch"},
	{Code: "en-us", Language: "en", Analyzer: "english"},
	{Code: "et-ee", Language: "et", Analyzer: "estonian"},
	{Code: "fi-fi", Language: "fi", Analyzer: "finnish"},
	{Code: "fr-fr", Language: "fr", Analyzer: "french"},
	{Code: "gl-es", Language: "gl", Analyzer: "galician"},
	{Code: "de-de", Language: "de", Analyzer: "german"},
	{Code: "el-gr", Language: "el", Analyzer: "greek"},
	{Code: "hi-in", Language: "hi", Analyzer: "hindi"},
	{Code: "hu-hu", Language: "hu", Analyzer: "hungarian"},
	{Code: "id-id", Language: "id", Analyzer: "indonesian"},
	{Code: "ga-ie", Language: "ga", Analyzer: "irish"},
	{Code: "it-it", Language: "it", Analyzer: "italian"},
	{Code: "lv-lv", Language: "lv", Analyzer: "latvian"},
	{Code: "lt-lt", Language: "lt", Analyzer: "lithuanian"},
	{Code: "no-no", Language: "no", Analyzer: "norwegian"},
	{Code: "fa-ir", Language: "fa", Analyzer: "persian"},
	{Code: "pt-pt", Language: "pt", Analyzer: "portuguese"},
	{Code: "ro-ro", Language: "ro", Analyzer: "romanian"},
	{Code: "ru-ru", Language: "ru", Analyzer: "russian"},
	{Code: "es-es", Language: "es", Analyzer: "spanish"},
	{Code: "sv-se", Language: "sv", Analyzer: "swedish"},
	{Code: "tr-tr", Language: "tr", Analyzer: "turkish"},
	{Code: "th-th", Language: "th", Analyzer: "thai"},
}
