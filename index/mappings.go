package index

type FieldType string

const (
	TypeUnknown    FieldType = ""
	TypeBoolean    FieldType = "boolean"
	TypeDouble     FieldType = "double"
	TypeLong       FieldType = "long"
	TypeDate       FieldType = "date"
	TypeText       FieldType = "text"
	TypeKeyword    FieldType = "keyword"
	TypeAlias      FieldType = "alias"
	TypePercolator FieldType = "percolator"
	TypeICUKeyword FieldType = "icu_collation_keyword"
)

// We should not have colliding types, but if something first is defined as text
// or keyword, and then has a more specific constraint in f.ex. an extension,
// then we should allow the more specific constraint to win out.
func (ft FieldType) Priority() int {
	switch ft {
	case TypeUnknown:
		return 0
	case TypeText:
		return 1
	case TypeKeyword:
		return 2
	case TypeDate:
		return 5
	case TypeBoolean:
		return 10
	case TypeDouble:
		return 11
	case TypeLong:
		return 12
	case TypeAlias:
		return 13
	case TypeICUKeyword:
		return 14
	case TypePercolator:
		return 20
	}

	return 0
}

type Field struct {
	Type   FieldType           `json:"type"`
	Values []string            `json:"values"`
	Fields map[string]SubField `json:"fields,omitempty"`
}

func (f *Field) AddSubField(name string, sf SubField) {
	if f.Fields == nil {
		f.Fields = make(map[string]SubField)
	}

	f.Fields[name] = sf
}

type SubField struct {
	Type           FieldType `json:"type"`
	Normalizer     string    `json:"normalizer,omitempty"`
	Analyzer       string    `json:"analyzer,omitempty"`
	SearchAnalyzer string    `json:"search_analyzer,omitempty"`
	Index          *bool     `json:"index,omitempty"`
	Language       string    `json:"language,omitempty"`
	Country        string    `json:"country,omitempty"`
	Variant        string    `json:"variant,omitempty"`
}

func (sf SubField) Equal(other SubField) bool {
	return other.Type == sf.Type &&
		other.Normalizer == sf.Normalizer &&
		other.Analyzer == sf.Analyzer &&
		other.SearchAnalyzer == sf.SearchAnalyzer &&
		other.Language == sf.Language &&
		other.Country == sf.Country &&
		other.Variant == sf.Variant &&
		equalPointerValue(other.Index, sf.Index)
}

func equalPointerValue[T comparable](a *T, b *T) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	return *a == *b
}

type Mapping struct {
	Type   FieldType           `json:"type"`
	Path   string              `json:"path,omitempty"`
	Fields map[string]SubField `json:"fields,omitempty"`
}

type MappingComparison string

const (
	MappingBreaking = "breaking"
	MappingEqual    = "equal"
	MappingSuperset = "superset"
)

func (m Mapping) Compare(other Mapping) MappingComparison {
	equal := other.Type == m.Type && other.Path == m.Path
	if !equal {
		return MappingBreaking
	}

	switch {
	case len(other.Fields) == 0 && len(m.Fields) == 0:
		// No subfields on either side.
		return MappingEqual
	case len(other.Fields) == 0:
		// New subfields.
		return MappingSuperset
	case len(m.Fields) == 0:
		// Cannot remove subfields.
		return MappingBreaking
	}

	for k, v := range other.Fields {
		nv, ok := m.Fields[k]
		if !ok {
			// Existing subfield has been removed.
			return MappingBreaking
		}

		if nv.Normalizer != v.Normalizer || nv.Type != v.Type {
			// Type of subfield has changed.
			return MappingBreaking
		}
	}

	var hasNewSubfields bool

	for k, v := range m.Fields {
		ov, ok := other.Fields[k]
		if !ok {
			hasNewSubfields = true

			continue
		}

		if ov.Normalizer != v.Normalizer || ov.Type != v.Type {
			return MappingBreaking
		}
	}

	if hasNewSubfields {
		return MappingSuperset
	}

	return MappingEqual
}

type Mappings struct {
	Properties map[string]Mapping `json:"properties"`
}

func NewMappings() Mappings {
	return Mappings{
		Properties: make(map[string]Mapping),
	}
}

type MappingChanges map[string]MappingChange

func (mc MappingChanges) HasNew() bool {
	for n := range mc {
		if mc[n].Comparison == MappingSuperset {
			return true
		}
	}

	return false
}

func (mc MappingChanges) Superset(mappings Mappings) Mappings {
	sup := Mappings{
		Properties: make(map[string]Mapping),
	}

	for k, v := range mappings.Properties {
		sup.Properties[k] = v
	}

	for k := range mc {
		// Only add changes that are part of a superset. We can't break
		// the current mappings.
		if mc[k].Comparison != MappingSuperset {
			continue
		}

		sup.Properties[k] = mc[k].Mapping
	}

	return sup
}

type MappingChange struct {
	Mapping

	Comparison MappingComparison `json:"comparison"`
}

func (m *Mappings) ChangesFrom(mappings Mappings) MappingChanges {
	changes := make(MappingChanges)

	for k, def := range m.Properties {
		original, ok := mappings.Properties[k]
		if !ok {
			changes[k] = MappingChange{
				Mapping:    def,
				Comparison: MappingSuperset,
			}

			continue
		}

		cmp := def.Compare(original)
		if cmp == MappingEqual {
			continue
		}

		changes[k] = MappingChange{
			Mapping:    def,
			Comparison: cmp,
		}
	}

	return changes
}
