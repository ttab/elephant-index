package index

import (
	"context"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/microcosm-cc/bluemonday"
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
)

func BuildDocument(
	validator *revisor.Validator, state *DocumentState,
	language OpenSeachIndexConfig, featureFlags map[string]bool,
) (*Document, error) {
	d := NewDocument()

	if state == nil {
		return d, nil
	}

	doc := &state.Document

	titleField := Field{
		Type:   TypeText,
		Values: []string{doc.Title},
	}

	if featureFlags[FeatureSortable] {
		addSortSubField(&titleField, language, nil)
	}

	if featureFlags[FeaturePrefix] {
		addPrefixSubField(&titleField)
	}

	// TODO: I'ts a bit awkward that we pre-declare these before running
	// collection. It should be treated like we do with all other fields.
	d.AddField("document.title", titleField)
	d.AddField("document.uri", Field{
		Type:   TypeKeyword,
		Values: []string{doc.URI},
	})
	d.AddField("document.url", Field{
		Type:   TypeKeyword,
		Values: []string{doc.URL},
	})
	d.AddField("document.language", Field{
		Type:   TypeKeyword,
		Values: []string{doc.Language},
	})

	d.AddInteger("current_version", state.CurrentVersion)
	d.AddTime("created", state.Created)
	d.AddTime("modified", state.Modified)

	for name, status := range state.Heads {
		base := "heads." + name

		d.AddInteger(base+".id", status.ID)
		d.AddInteger(base+".version", status.Version)
		d.AddField(base+".creator", Field{
			Type:   TypeKeyword,
			Values: []string{status.Creator},
		})
		d.AddTime(base+".created", status.Created)

		for k, v := range status.Meta {
			d.AddField(base+".meta."+k, Field{
				Type:   TypeKeyword,
				Values: []string{v},
			})
		}
	}

	for _, a := range state.ACL {
		if !slices.Contains(a.Permissions, "r") {
			continue
		}

		d.AddField("readers", Field{
			Type:   TypeKeyword,
			Values: []string{a.URI},
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
		Type:   TypeText,
		Values: text,
	})

	err := collectDocumentFields(
		d, "document.", doc, validator, language, featureFlags, policy)
	if err != nil {
		return nil, fmt.Errorf("main document: %w", err)
	}

	if state.MetaDocument != nil {
		err := collectDocumentFields(
			d, "meta.", state.MetaDocument, validator,
			language, featureFlags, policy,
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
	language OpenSeachIndexConfig, featureFlags map[string]bool, policy *bluemonday.Policy,
) error {
	coll := NewValueCollector()

	_, err := validator.ValidateDocument(
		context.Background(),
		doc,
		revisor.WithValueCollector(coll))
	if err != nil {
		return fmt.Errorf("could not collect values: %w", err)
	}

	for _, a := range coll.Values() {
		var ft FieldType

		val := a.Value

		switch {
		case a.Constraint.Format == revisor.StringFormatFloat:
			ft = TypeDouble
		case a.Constraint.Format == revisor.StringFormatInt:
			ft = TypeLong
		case a.Constraint.Format == revisor.StringFormatBoolean:
			ft = TypeBoolean
		case a.Constraint.Format == revisor.StringFormatUUID:
			ft = TypeKeyword
		case a.Constraint.Format == revisor.StringFormatRFC3339:
			ft = TypeDate
		case a.Constraint.Format == revisor.StringFormatHTML:
			ft = TypeText
			val = policy.Sanitize(val)
			val = html.UnescapeString(val)
		case a.Constraint.Time != "":
			ft = TypeDate

			t, err := time.Parse(a.Constraint.Time, val)
			if err == nil {
				val = t.Format(time.RFC3339)
			}
		case len(a.Constraint.Enum) > 0:
			ft = TypeKeyword
		case a.Constraint.Pattern != nil:
			ft = TypeKeyword
		case len(a.Constraint.Glob) > 0:
			ft = TypeKeyword
		default:
			ft = TypeText
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
		if tail.RefType == revisor.RefTypeAttribute && ft == TypeText {
			switch tail.Name {
			case "title", "value":
			default:
				ft = TypeKeyword
			}
		}

		path := prefix + entityRefsToPath(doc, a.Ref)

		var aliases []string

		if a.Constraint.Hints != nil {
			aliases = a.Constraint.Hints["alias"]
		}

		f := Field{
			Type:   ft,
			Values: []string{val},
		}

		if slices.Contains(a.Constraint.Labels, "keyword") {
			f.AddSubField("keyword", SubField{
				Type: TypeKeyword,
			})
		}

		if featureFlags[FeatureSortable] {
			if slices.Contains(a.Constraint.Labels, "sortable") {
				addSortSubField(&f, language, a.Constraint.Labels)
			}
		}

		if featureFlags[FeaturePrefix] {
			if slices.Contains(a.Constraint.Labels, "prefix") {
				addPrefixSubField(&f)
			}
		}

		d.AddField(path, f)

		for _, alias := range aliases {
			// TODO: can we alias the sub-fields as well, is it
			// needed or is an alias like a directory symlink?
			d.AddField(alias, Field{
				Type:   TypeAlias,
				Values: []string{path},
			})
		}
	}

	return nil
}

func addPrefixSubField(field *Field) {
	field.AddSubField("prefix", SubField{
		Type:           TypeText,
		Analyzer:       "elephant_prefix_analyzer",
		SearchAnalyzer: "elephant_prefix_search_analyzer",
	})
}

func addSortSubField(field *Field, language OpenSeachIndexConfig, labels []string) {
	var variant string

	if language.Language == "de" && slices.Contains(labels, "de-phonebook") {
		variant = "@collation=phonebook"
	}

	field.AddSubField("sort", SubField{
		Type:     TypeICUKeyword,
		Language: language.Language,
		Country:  language.Region,
		Variant:  variant,
	})
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

var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

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

			switch v.BlockKind {
			case revisor.BlockKindLink:
				key := nonAlphaNum.ReplaceAllString(block.Rel, "_")
				r[i] = "rel." + key
			case revisor.BlockKindMeta:
				key := nonAlphaNum.ReplaceAllString(block.Type, "_")
				r[i] = "meta." + key
			case revisor.BlockKindContent:
				key := nonAlphaNum.ReplaceAllString(block.Type, "_")
				r[i] = "content." + key
			}

			source = revisor.NewNestedBlocks(&block)
		}
	}

	return strings.Join(r, ".")
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
