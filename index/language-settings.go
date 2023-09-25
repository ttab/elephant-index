package index

import (
	"strings"

	"golang.org/x/text/language"
)

type LanguageSettings struct {
	Tag      language.Tag
	Settings string
}

type Settings struct {
	Name     string
	Language string
	Settings string
}

func GetIndexSettings(code string) (Settings, error) {
	tag, i := language.MatchStrings(languages, code)
	if i == 0 {
		return Settings{
			Name:     "standard",
			Language: "",
			Settings: "",
		}, nil
	}

	lang, _ := tag.Base()

	return Settings{
		Name:     strings.ToLower(code),
		Language: lang.String(),
		Settings: languageSettings[i].Settings,
	}, nil
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
		Settings: "",
	},
	{
		Tag:      language.Arabic,
		Settings: makeSettings("arabic"),
	},
	{
		Tag:      language.Armenian,
		Settings: makeSettings("armenian"),
	},
	{
		Tag:      language.Make("eu"),
		Settings: makeSettings("basque"),
	},
	{
		Tag:      language.Bengali,
		Settings: makeSettings("bengali"),
	},
	{
		Tag:      language.BrazilianPortuguese,
		Settings: makeSettings("brazilian"),
	},
	{
		Tag:      language.Bulgarian,
		Settings: makeSettings("bulgarian"),
	},
	{
		Tag:      language.Catalan,
		Settings: makeSettings("catalan"),
	},
	{
		Tag:      language.Czech,
		Settings: makeSettings("czech"),
	},
	{
		Tag:      language.Danish,
		Settings: makeSettings("danish"),
	},
	{
		Tag:      language.Dutch,
		Settings: makeSettings("dutch"),
	},
	{
		Tag:      language.English,
		Settings: makeSettings("english"),
	},
	{
		Tag:      language.Estonian,
		Settings: makeSettings("estonian"),
	},
	{
		Tag:      language.Finnish,
		Settings: makeSettings("finnish"),
	},
	{
		Tag:      language.French,
		Settings: makeSettings("french"),
	},
	{
		Tag:      language.Make("gl"),
		Settings: makeSettings("galician"),
	},
	{
		Tag:      language.German,
		Settings: makeSettings("german"),
	},
	{
		Tag:      language.Greek,
		Settings: makeSettings("greek"),
	},
	{
		Tag:      language.Hindi,
		Settings: makeSettings("hindi"),
	},
	{
		Tag:      language.Hungarian,
		Settings: makeSettings("hungarian"),
	},
	{
		Tag:      language.Indonesian,
		Settings: makeSettings("indonesian"),
	},
	{
		Tag:      language.Make("ga"),
		Settings: makeSettings("irish"),
	},
	{
		Tag:      language.Italian,
		Settings: makeSettings("italian"),
	},
	{
		Tag:      language.Latvian,
		Settings: makeSettings("latvian"),
	},
	{
		Tag:      language.Lithuanian,
		Settings: makeSettings("lithuanian"),
	},
	{
		Tag:      language.Norwegian,
		Settings: makeSettings("norwegian"),
	},
	{
		Tag:      language.Persian,
		Settings: makeSettings("persian"),
	},
	{
		Tag:      language.EuropeanPortuguese,
		Settings: makeSettings("portuguese"),
	},
	{
		Tag:      language.Romanian,
		Settings: makeSettings("romanian"),
	},
	{
		Tag:      language.Russian,
		Settings: makeSettings("russian"),
	},
	{
		Tag:      language.Spanish,
		Settings: makeSettings("spanish"),
	},
	{
		Tag:      language.Swedish,
		Settings: makeSettings("swedish"),
	},
	{
		Tag:      language.Turkish,
		Settings: makeSettings("turkish"),
	},
	{
		Tag:      language.Thai,
		Settings: makeSettings("thai"),
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
