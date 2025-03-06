package index

import (
	"strconv"
	"time"

	"github.com/ttab/newsdoc"
	"golang.org/x/exp/slices"
)

// DocumentState is the full state that we want to index.
type DocumentState struct {
	Created            time.Time         `json:"created"`
	Creator            string            `json:"creator"`
	Modified           time.Time         `json:"modified"`
	Updater            string            `json:"updater"`
	CurrentVersion     int64             `json:"current_version"`
	ACL                []ACLEntry        `json:"acl"`
	Heads              map[string]Status `json:"heads"`
	Document           newsdoc.Document  `json:"document"`
	MetaDocument       *newsdoc.Document `json:"meta_document"`
	WorkflowState      string            `json:"workflow_state"`
	WorkflowCheckpoint string            `json:"workflow_checkpoint"`
}

type Document struct {
	Fields map[string]Field
}

func NewDocument() *Document {
	return &Document{
		Fields: make(map[string]Field),
	}
}

func (d *Document) AddTime(name string, value time.Time) {
	v := value.Format(time.RFC3339)

	d.AddField(name, Field{
		FieldOptions: FieldOptions{Type: TypeDate},
		Values:       []string{v},
	})
}

func (d *Document) AddInteger(name string, value int64) {
	v := strconv.FormatInt(value, 10)

	d.AddField(name, Field{
		FieldOptions: FieldOptions{Type: TypeLong},
		Values:       []string{v},
	})
}

func (d *Document) AddField(name string, f Field) {
	e := d.Fields[name]

	if f.Type.Priority() > e.Type.Priority() {
		e.FieldOptions = f.FieldOptions
	}

	for name, def := range f.Fields {
		e.AddSubField(name, def)
	}

	for _, value := range f.Values {
		if !slices.Contains(e.Values, value) {
			e.Values = append(e.Values, value)
		}
	}

	d.Fields[name] = e
}

func (d *Document) Mappings() Mappings {
	m := Mappings{
		Properties: make(map[string]Mapping),
	}

	for name, def := range d.Fields {
		switch def.Type { //nolint: exhaustive
		case TypeAlias:
			m.Properties[name] = Mapping{
				FieldOptions: def.FieldOptions,
				Path:         def.Values[0],
				Fields:       def.Fields,
			}
		default:
			m.Properties[name] = Mapping{
				FieldOptions: def.FieldOptions,
				Fields:       def.Fields,
			}
		}
	}

	return m
}

func (d *Document) Values() map[string][]string {
	v := make(map[string][]string)

	for k := range d.Fields {
		if d.Fields[k].Type == TypeAlias {
			continue
		}

		v[k] = d.Fields[k].Values
	}

	return v
}
