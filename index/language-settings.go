package index

import (
	"fmt"
	"strings"
)

type LanguageConfig struct {
	NameSuffix string
	Language   string
	Settings   OpensearchSettings
}

type OpensearchSettings struct {
	Settings struct {
		Analysis struct {
			Analyzer struct {
				Default struct {
					Type string `json:"type"`
				} `json:"default"`
			} `json:"analyzer"`
		} `json:"analysis"`
	} `json:"settings"`
}

func GetLanguageConfig(code string) (LanguageConfig, error) {
	code = strings.ToLower(code)
	parts := strings.Split(code, "-")
	if len(parts) < 1 || len(parts) > 2 {
		return LanguageConfig{}, fmt.Errorf("malformed language code: %s", code)
	}

	lang := parts[0]

	region := "unspecified"
	if len(parts) > 1 {
		region = parts[1]
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

	s := OpensearchSettings{}
	s.Settings.Analysis.Analyzer.Default.Type = analyzer

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
