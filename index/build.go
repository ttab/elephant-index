package index

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/microcosm-cc/bluemonday"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
	"golang.org/x/exp/slices"
)

// Fields can depend on index settings (like custom normalisers). These won't be
// supported on old indexes, so instead of failing we set feature flags on the
// indexes so that we know what's supported.
const (
	FeatureSortable = "sortable"
	FeaturePrefix   = "prefix"
	FeatureOnlyICU  = "only_icu"
)

func BuildDocument(
	validator *revisor.Validator, state *DocumentState,
	conf OpenSearchIndexConfig, featureFlags map[string]bool,
) (*Document, error) {
	d := NewDocument()

	if state == nil {
		return d, nil
	}

	doc := &state.Document

	titleField := Field{
		FieldOptions: FieldOptions{Type: TypeText},
		Values:       []string{doc.Title},
	}

	if featureFlags[FeatureSortable] {
		titleField.AddSubField("sort",
			collatedKeywordOptions(conf.Language, nil))
	}

	if featureFlags[FeaturePrefix] {
		titleField.AddSubField("prefix", prefixFieldOptions())
	}

	keywordOptions := FieldOptions{Type: TypeKeyword}
	if featureFlags[FeatureOnlyICU] {
		keywordOptions = collatedKeywordOptions(conf.Language, nil)
	}

	// TODO: I'ts a bit awkward that we pre-declare these before running
	// collection. It should be treated like we do with all other fields.
	d.AddField("document.title", titleField)
	d.AddField("document.uri", Field{
		FieldOptions: keywordOptions,
		Values:       []string{doc.URI},
	})
	d.AddField("document.url", Field{
		FieldOptions: keywordOptions,
		Values:       []string{doc.URL},
	})
	d.AddField("document.language", Field{
		FieldOptions: keywordOptions,
		Values:       []string{doc.Language},
	})

	d.AddInteger("current_version", state.CurrentVersion)
	d.AddTime("created", state.Created)
	d.AddTime("modified", state.Modified)

	d.AddField("creator_uri", Field{
		FieldOptions: keywordOptions,
		Values:       []string{state.Creator},
	})

	d.AddField("updater_uri", Field{
		FieldOptions: keywordOptions,
		Values:       []string{state.Updater},
	})

	if state.WorkflowState != "" {
		d.AddField("workflow_state", Field{
			FieldOptions: keywordOptions,
			Values:       []string{state.WorkflowState},
		})
	}

	if state.WorkflowCheckpoint != "" {
		d.AddField("workflow_checkpoint", Field{
			FieldOptions: keywordOptions,
			Values:       []string{state.WorkflowCheckpoint},
		})
	}

	for name, status := range state.Heads {
		base := "heads." + name

		d.AddInteger(base+".id", status.ID)
		d.AddInteger(base+".version", status.Version)
		d.AddField(base+".creator", Field{
			FieldOptions: keywordOptions,
			Values:       []string{status.Creator},
		})
		d.AddTime(base+".created", status.Created)

		for k, v := range status.Meta {
			d.AddField(base+".meta."+k, Field{
				FieldOptions: keywordOptions,
				Values:       []string{v},
			})
		}
	}

	for _, a := range state.ACL {
		if !slices.Contains(a.Permissions, "r") {
			continue
		}

		d.AddField("readers", Field{
			FieldOptions: keywordOptions,
			Values:       []string{a.URI},
		})
	}

	policy := bluemonday.StrictPolicy()

	text := []string{doc.Title}

	for i := range doc.Content {
		text = blockText(policy, doc.Content[i], text)
	}

	for i := range doc.Meta {
		text = blockText(policy, doc.Meta[i], text)
	}

	d.AddField("text", Field{
		FieldOptions: FieldOptions{Type: TypeText},
		Values:       text,
	})

	err := collectDocumentFields(
		d, "document.", doc, validator, conf.Language, featureFlags, policy)
	if err != nil {
		return nil, fmt.Errorf("main document: %w", err)
	}

	if state.MetaDocument != nil {
		err := collectDocumentFields(
			d, "meta.", state.MetaDocument, validator,
			conf.Language, featureFlags, policy,
		)
		if err != nil {
			return nil, fmt.Errorf("meta document: %w", err)
		}
	}

	return d, nil
}

func collectDocumentFields(
	d *Document, prefix string, doc *newsdoc.Document,
	validator *revisor.Validator,
	language LanguageInfo, featureFlags map[string]bool, policy *bluemonday.Policy,
) error {
	coll := NewValueCollector()

	keywordOptions := collatedKeywordOptions(language, nil)

	onlyICU := featureFlags[FeatureOnlyICU]
	if !onlyICU {
		keywordOptions = FieldOptions{Type: TypeKeyword}
	}

	_, err := validator.ValidateDocument(
		context.Background(),
		doc,
		revisor.WithValueCollector(coll))
	if err != nil {
		return fmt.Errorf("could not collect values: %w", err)
	}

	for _, a := range coll.Values() {
		var fo FieldOptions

		val := a.Value

		switch {
		case a.Constraint.Format == revisor.StringFormatFloat:
			fo.Type = TypeDouble
		case a.Constraint.Format == revisor.StringFormatInt:
			fo.Type = TypeLong
		case a.Constraint.Format == revisor.StringFormatBoolean:
			fo.Type = TypeBoolean
		case a.Constraint.Format == revisor.StringFormatUUID:
			fo.Type = TypeKeyword
		case a.Constraint.Format == revisor.StringFormatRFC3339:
			fo.Type = TypeDate
		case a.Constraint.Format == revisor.StringFormatHTML:
			fo.Type = TypeText
			val = policy.Sanitize(val)
			val = html.UnescapeString(val)
		case a.Constraint.Time != "":
			fo.Type = TypeDate

			t, err := time.Parse(a.Constraint.Time, val)
			if err == nil {
				val = t.Format(time.RFC3339)
			}
		case len(a.Constraint.Enum) > 0:
			fo = keywordOptions
		case a.Constraint.Pattern != nil:
			fo = keywordOptions
		case len(a.Constraint.Glob) > 0:
			fo = keywordOptions
		default:
			fo.Type = TypeText
		}

		var parent revisor.EntityRef

		if len(a.Ref) > 1 {
			parent = a.Ref[len(a.Ref)-2]
		}

		tail := a.Ref[len(a.Ref)-1]

		// Omit rel here as it's part of the link field name.
		if isKind(parent, revisor.BlockKindLink) &&
			tail.RefType == revisor.RefTypeAttribute &&
			tail.Name == "rel" {
			continue
		}

		// Omit rel here as it's part of meta and content field names.
		if isKind(parent, revisor.BlockKindMeta, revisor.BlockKindContent) &&
			tail.RefType == revisor.RefTypeAttribute &&
			tail.Name == "type" {
			continue
		}

		// All attributes except title and value should default to
		// keyword if they're just text.
		if tail.RefType == revisor.RefTypeAttribute && fo.Type == TypeText {
			switch tail.Name {
			case "title", "value":
			default:
				fo = keywordOptions
			}
		}

		path := prefix + entityRefsToPath(doc, a.Ref)

		var aliases []string

		if a.Constraint.Hints != nil {
			aliases = a.Constraint.Hints["alias"]
		}

		f := Field{
			FieldOptions: fo,
			Values:       []string{val},
		}

		if slices.Contains(a.Constraint.Labels, "keyword") {
			if onlyICU {
				f.AddSubField("keyword", collatedKeywordOptions(
					language, a.Constraint.Labels))
			} else {
				f.AddSubField("keyword", FieldOptions{
					Type: TypeKeyword,
				})
			}
		}

		if featureFlags[FeatureSortable] &&
			slices.Contains(a.Constraint.Labels, "sortable") {
			f.AddSubField("sort", collatedKeywordOptions(
				language, a.Constraint.Labels))
		}

		if featureFlags[FeaturePrefix] &&
			slices.Contains(a.Constraint.Labels, "prefix") {
			f.AddSubField("prefix", prefixFieldOptions())
		}

		d.AddField(path, f)

		for _, alias := range aliases {
			// TODO: can we alias the sub-fields as well, is it
			// needed or is an alias like a directory symlink?
			d.AddField(alias, Field{
				FieldOptions: FieldOptions{
					Type: TypeAlias,
				},
				Values: []string{path},
			})
		}
	}

	return nil
}

func prefixFieldOptions() FieldOptions {
	return FieldOptions{
		Type:           TypeText,
		Analyzer:       "elephant_prefix_analyzer",
		SearchAnalyzer: "elephant_prefix_search_analyzer",
	}
}

func collatedKeywordOptions(
	language LanguageInfo, labels []string,
) FieldOptions {
	var variant string

	if language.Language == "de" && slices.Contains(labels, "de-phonebook") {
		variant = "@collation=phonebook"
	}

	return FieldOptions{
		Type:     TypeICUKeyword,
		Language: language.Language,
		Country:  language.Region,
		Variant:  variant,
	}
}

func blockText(policy *bluemonday.Policy, b newsdoc.Block, text []string) []string {
	if b.Title != "" {
		text = append(text, html.UnescapeString(policy.Sanitize(b.Title)))
	}

	if b.Data != nil {
		t := b.Data["text"]
		if t != "" {
			text = append(text, html.UnescapeString(policy.Sanitize(t)))
		}
	}

	for i := range b.Content {
		text = blockText(policy, b.Content[i], text)
	}

	return text
}

func isKind(r revisor.EntityRef, kind ...revisor.BlockKind) bool {
	if r.RefType != revisor.RefTypeBlock {
		return false
	}

	for _, k := range kind {
		if r.BlockKind == k {
			return true
		}
	}

	return false
}

func entityRefsToPath(doc *newsdoc.Document, refs []revisor.EntityRef) string {
	r := make([]string, len(refs))

	var source revisor.BlockSource = revisor.NewDocumentBlocks(doc)

	for i, v := range refs {
		switch v.RefType {
		case revisor.RefTypeData:
			r[i] = "data." + v.Name
		case revisor.RefTypeAttribute:
			r[i] = v.Name
		case revisor.RefTypeBlock:
			blocks := source.GetBlocks(v.BlockKind)
			block := blocks[v.Index]

			values := map[string]string{
				"type": block.Type,
				"rel":  block.Rel,
				"role": block.Role,
				"name": block.Name,
			}

			fallbacks := []string{
				"type", "rel", "role", "name",
			}

			switch v.BlockKind {
			case revisor.BlockKindLink:
				key := blockKeyWithFallback("rel", values, fallbacks...)
				r[i] = "rel." + key
			case revisor.BlockKindMeta:
				key := blockKeyWithFallback("type", values, fallbacks...)
				r[i] = "meta." + key
			case revisor.BlockKindContent:
				key := blockKeyWithFallback("type", values, fallbacks...)
				r[i] = "content." + key
			}

			source = revisor.NewNestedBlocks(&block)
		}
	}

	return strings.Join(r, ".")
}

func blockKeyWithFallback(
	primary string, values map[string]string, fallbacks ...string,
) string {
	v := values[primary]
	if v != "" {
		return internal.NonAlphaNum.ReplaceAllString(v, "_")
	}

	for _, k := range fallbacks {
		v := values[k]
		if v != "" {
			return k + "__" + internal.NonAlphaNum.ReplaceAllString(v, "_")
		}
	}

	return "__unknown"
}

type ValueCollector struct {
	c    *collectorAnnotations
	path []revisor.EntityRef
}

type collectorAnnotations struct {
	List []revisor.ValueAnnotation
}

func NewValueCollector() *ValueCollector {
	return &ValueCollector{
		c: &collectorAnnotations{},
	}
}

func (c *ValueCollector) CollectValue(a revisor.ValueAnnotation) {
	a.Ref = append(c.path[0:len(c.path):len(c.path)], a.Ref...)
	c.c.List = append(c.c.List, a)
}

func (c *ValueCollector) With(ref revisor.EntityRef) revisor.ValueCollector {
	n := ValueCollector{
		c:    c.c,
		path: append(c.path[0:len(c.path):len(c.path)], ref),
	}

	return &n
}

func (c *ValueCollector) Values() []revisor.ValueAnnotation {
	return c.c.List
}

var _ revisor.ValueCollector = &ValueCollector{}
