package index

import (
	"strings"

	"golang.org/x/text/language"
)

type LanguageSettings struct {
	Tag      language.Tag
	Analyzer string
}

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

func GetLanguageConfig(code string) LanguageConfig {
	tag, i := language.MatchStrings(languages, code)
	if i == 0 {
		tag = language.Make(code)
	}

	lang, _ := tag.Base()

	s := OpensearchSettings{}
	s.Settings.Analysis.Analyzer.Default.Type = languageSettings[i].Analyzer

	return LanguageConfig{
		Name:     strings.ToLower(code),
		Language: lang.String(),
		Settings: s,
	}
}

var languages = (func() language.Matcher {
	tags := []language.Tag{}
	for _, lang := range languageSettings {
		tags = append(tags, lang.Tag)
	}

	return language.NewMatcher(tags)
})()

// These are the language-specific settings that Opensearch can handle.
var languageSettings = []LanguageSettings{
	{
		Tag:      language.Tag{},
		Analyzer: "standard",
	},
	{
		Tag:      language.Arabic,
		Analyzer: "arabic",
	},
	{
		Tag:      language.Armenian,
		Analyzer: "armenian",
	},
	{
		Tag:      language.Make("eu"),
		Analyzer: "basque",
	},
	{
		Tag:      language.Bengali,
		Analyzer: "bengali",
	},
	{
		Tag:      language.BrazilianPortuguese,
		Analyzer: "brazilian",
	},
	{
		Tag:      language.Bulgarian,
		Analyzer: "bulgarian",
	},
	{
		Tag:      language.Catalan,
		Analyzer: "catalan",
	},
	{
		Tag:      language.Czech,
		Analyzer: "czech",
	},
	{
		Tag:      language.Danish,
		Analyzer: "danish",
	},
	{
		Tag:      language.Dutch,
		Analyzer: "dutch",
	},
	{
		Tag:      language.English,
		Analyzer: "english",
	},
	{
		Tag:      language.Estonian,
		Analyzer: "estonian",
	},
	{
		Tag:      language.Finnish,
		Analyzer: "finnish",
	},
	{
		Tag:      language.French,
		Analyzer: "french",
	},
	{
		Tag:      language.Make("gl"),
		Analyzer: "galician",
	},
	{
		Tag:      language.German,
		Analyzer: "german",
	},
	{
		Tag:      language.Greek,
		Analyzer: "greek",
	},
	{
		Tag:      language.Hindi,
		Analyzer: "hindi",
	},
	{
		Tag:      language.Hungarian,
		Analyzer: "hungarian",
	},
	{
		Tag:      language.Indonesian,
		Analyzer: "indonesian",
	},
	{
		Tag:      language.Make("ga"),
		Analyzer: "irish",
	},
	{
		Tag:      language.Italian,
		Analyzer: "italian",
	},
	{
		Tag:      language.Latvian,
		Analyzer: "latvian",
	},
	{
		Tag:      language.Lithuanian,
		Analyzer: "lithuanian",
	},
	{
		Tag:      language.Norwegian,
		Analyzer: "norwegian",
	},
	{
		Tag:      language.Persian,
		Analyzer: "persian",
	},
	{
		Tag:      language.EuropeanPortuguese,
		Analyzer: "portuguese",
	},
	{
		Tag:      language.Romanian,
		Analyzer: "romanian",
	},
	{
		Tag:      language.Russian,
		Analyzer: "russian",
	},
	{
		Tag:      language.Spanish,
		Analyzer: "spanish",
	},
	{
		Tag:      language.Swedish,
		Analyzer: "swedish",
	},
	{
		Tag:      language.Turkish,
		Analyzer: "turkish",
	},
	{
		Tag:      language.Thai,
		Analyzer: "thai",
	},
}

func makeSettings(analyzer string) string {
	return `{
		"settings": {
			"analysis": {
				"analyzer": {
					"default": {
						"type": "` + analyzer + `"
					}
				}
			}
		}
	}`
}
