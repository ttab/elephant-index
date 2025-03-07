package index

import (
	"fmt"
	"strings"

	"github.com/ttab/langos"
)

type OpenSearchIndexConfig struct {
	Language LanguageInfo
	Settings OpensearchSettings
}

type OpensearchSettings struct {
	Settings struct {
		Index    ShardingSettings   `json:"index"`
		Analysis OpensearchAnalysis `json:"analysis"`
	} `json:"settings"`
}

type OpensearchAnalysis struct {
	Analyzer   map[string]OpensearchAnalyzer   `json:"analyzer,omitempty"`
	Normalizer map[string]OpensearchNormaliser `json:"normalizer,omitempty"`
	Tokenizer  map[string]OpensearchTokenizer  `json:"tokenizer,omitempty"`
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

func (osa *OpensearchAnalysis) SetTokenizer(name string, v OpensearchTokenizer) {
	if osa.Tokenizer == nil {
		osa.Tokenizer = make(map[string]OpensearchTokenizer)
	}

	osa.Tokenizer[name] = v
}

type OpensearchAnalyzer struct {
	Type      string   `json:"type"`
	Tokenizer string   `json:"tokenizer,omitempty"`
	Filter    []string `json:"filter,omitempty"`
}

type OpensearchNormaliser struct {
	Type   string   `json:"type"`
	Filter []string `json:"filter"`
}

type OpensearchTokenizer struct {
	Type       string   `json:"type"`
	MinGram    int      `json:"min_gram,omitempty"`
	MaxGram    int      `json:"max_gram,omitempty"`
	TokenChars []string `json:"token_chars,omitempty"`
}

func NewLanguageResolver(opts LanguageOptions) *LanguageResolver {
	if opts.DefaultRegions == nil {
		opts.DefaultRegions = make(map[string]string)
	}

	if opts.Substitutions == nil {
		opts.Substitutions = make(map[string]string)
	}

	return &LanguageResolver{
		opts:   opts,
		lTable: make(map[string]lrItem),
	}
}

type LanguageResolver struct {
	opts   LanguageOptions
	lTable map[string]lrItem
}

type lrItem struct {
	Info LanguageInfo
	Err  error
}

func (lr *LanguageResolver) GetLanguageInfo(code string) (LanguageInfo, error) {
	item, ok := lr.lTable[code]
	if ok {
		return item.Info, item.Err
	}

	info, err := getLanguageInfo(code, lr.opts)

	lr.lTable[code] = lrItem{
		Info: info,
		Err:  err,
	}

	return info, err
}

type LanguageInfo struct {
	Code         string
	Language     string
	Region       string
	RegionSuffix string
}

func getLanguageInfo(
	code string, opts LanguageOptions,
) (LanguageInfo, error) {
	if code == "" {
		code = opts.DefaultLanguage
	}

	sub, ok := opts.Substitutions[code]
	if ok {
		code = sub
	}

	info, err := langos.GetLanguage(code)
	if err != nil {
		return LanguageInfo{}, fmt.Errorf("get language: %w", err)
	}

	lang := LanguageInfo{
		Code:     strings.ToLower(info.Code),
		Language: info.Language,
	}

	if info.HasRegion {
		lang.Region = strings.ToLower(info.Region)
	} else {
		region, hasDefault := opts.DefaultRegions[info.Language]
		if hasDefault {
			lang.Region = strings.ToLower(region)
			lang.Code += "-" + lang.Region
		}
	}

	regionSuffix := "unspecified"
	if lang.Region != "" {
		regionSuffix = strings.ToLower(lang.Region)
	}

	lang.RegionSuffix = regionSuffix

	return lang, nil
}

func GetIndexConfig(
	lang LanguageInfo,
) OpenSearchIndexConfig {
	analyzer := "standard"

	for _, ls := range languages {
		if ls.Code == lang.Code {
			analyzer = ls.Analyzer

			break
		}

		if ls.Language == lang.Language {
			analyzer = ls.Analyzer
		}
	}

	var s OpensearchSettings

	s.Settings.Analysis.SetAnalyzer("default", OpensearchAnalyzer{
		Type: analyzer,
	})

	// Set up tokenizer and analyzer for prefix indexing. Not using the
	// "index_prefixes" mapping parameter as that only works for fields
	// typed as text. Adding a text subfield with "index_prefixes" would
	// give use text indexing AND prefix indexing. Instead we go for
	// subfields dedicated to prefix indexing.
	s.Settings.Analysis.SetTokenizer("edge_ngram_tokenizer", OpensearchTokenizer{
		Type:    "edge_ngram",
		MinGram: 2,
		MaxGram: 15,
	})

	s.Settings.Analysis.SetAnalyzer("elephant_prefix_analyzer", OpensearchAnalyzer{
		Type:      "custom",
		Tokenizer: "edge_ngram_tokenizer",
		Filter:    []string{"lowercase"},
	})

	// The prefix search analyzer skips tokenization but ensures lowercase
	// so that we get case insensitivity.
	s.Settings.Analysis.SetAnalyzer("elephant_prefix_search_analyzer", OpensearchAnalyzer{
		Type:      "custom",
		Tokenizer: "keyword",
		Filter:    []string{"lowercase"},
	})

	return OpenSearchIndexConfig{
		Language: lang,
		Settings: s,
	}
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
