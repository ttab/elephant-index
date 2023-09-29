package index

import (
	"fmt"
	"strings"
)

type LanguageConfig struct {
	Name     string
	Language string
	Settings OpensearchSettings
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
	parts := strings.Split(code, "-")
	if len(parts) != 2 {
		return LanguageConfig{}, fmt.Errorf("malformed language code: %s", code)
	}

	lang := parts[0]
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
		Name:     strings.ToLower(code),
		Language: lang,
		Settings: s,
	}, nil
}

type Language struct {
	Code     string
	Language string
	Analyzer string
}

var languages = []Language{
	{Code: "ar-EG", Language: "ar", Analyzer: "arabic"},
	{Code: "hy-AM", Language: "hy", Analyzer: "armenian"},
	{Code: "eu-ES", Language: "eu", Analyzer: "basque"},
	{Code: "bn-BD", Language: "bn", Analyzer: "bengali"},
	{Code: "pt-BR", Language: "pt", Analyzer: "brazilian"},
	{Code: "bg-BG", Language: "bg", Analyzer: "bulgarian"},
	{Code: "ca-ES", Language: "ca", Analyzer: "catalan"},
	{Code: "cs-CZ", Language: "cs", Analyzer: "czech"},
	{Code: "da-DK", Language: "da", Analyzer: "danish"},
	{Code: "nl-NL", Language: "nl", Analyzer: "dutch"},
	{Code: "en-US", Language: "en", Analyzer: "english"},
	{Code: "et-EE", Language: "et", Analyzer: "estonian"},
	{Code: "fi-FI", Language: "fi", Analyzer: "finnish"},
	{Code: "fr-FR", Language: "fr", Analyzer: "french"},
	{Code: "gl-ES", Language: "gl", Analyzer: "galician"},
	{Code: "de-DE", Language: "de", Analyzer: "german"},
	{Code: "el-GR", Language: "el", Analyzer: "greek"},
	{Code: "hi-IN", Language: "hi", Analyzer: "hindi"},
	{Code: "hu-HU", Language: "hu", Analyzer: "hungarian"},
	{Code: "id-ID", Language: "id", Analyzer: "indonesian"},
	{Code: "ga-IE", Language: "ga", Analyzer: "irish"},
	{Code: "it-IT", Language: "it", Analyzer: "italian"},
	{Code: "lv-LV", Language: "lv", Analyzer: "latvian"},
	{Code: "lt-LT", Language: "lt", Analyzer: "lithuanian"},
	{Code: "no-NO", Language: "no", Analyzer: "norwegian"},
	{Code: "fa-IR", Language: "fa", Analyzer: "persian"},
	{Code: "pt-PT", Language: "pt", Analyzer: "portuguese"},
	{Code: "ro-RO", Language: "ro", Analyzer: "romanian"},
	{Code: "ru-RU", Language: "ru", Analyzer: "russian"},
	{Code: "es-ES", Language: "es", Analyzer: "spanish"},
	{Code: "sv-SE", Language: "sv", Analyzer: "swedish"},
	{Code: "tr-TR", Language: "tr", Analyzer: "turkish"},
	{Code: "th-TH", Language: "th", Analyzer: "thai"},
}
