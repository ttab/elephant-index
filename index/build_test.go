package index_test

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine"
	"github.com/ttab/revisor"
	"github.com/ttab/revisorschemas"
)

func TestBuildDocument(t *testing.T) {
	var (
		state          index.DocumentState
		baseMappings   index.Mappings
		goldenMappings index.Mappings
		golden         map[string]index.Field
		goldenChanges  index.MappingChanges
	)

	err := elephantine.UnmarshalFile(
		"testdata/raw_1.input.json", &state)
	if err != nil {
		t.Fatalf("failed to load state data: %v", err)
	}

	err = elephantine.UnmarshalFile(
		"testdata/raw_1.fields.json", &golden)
	if err != nil {
		t.Fatalf("failed to load golden state: %v", err)
	}

	err = elephantine.UnmarshalFile(
		"testdata/mapping_subset.json", &baseMappings)
	if err != nil {
		t.Fatalf("failed to load base mappings: %v", err)
	}

	err = elephantine.UnmarshalFile(
		"testdata/mapping_superset.json", &goldenMappings)
	if err != nil {
		t.Fatalf("failed to load golden mappings: %v", err)
	}

	err = elephantine.UnmarshalFile(
		"testdata/mapping_changes.json", &goldenChanges)
	if err != nil {
		t.Fatalf("failed to load golden mapping changes: %v", err)
	}

	constraints, err := decodeConstraintSetsFS(revisorschemas.Files(),
		"core.json", "core-planning.json", "tt.json")
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	validator, err := revisor.NewValidator(constraints...)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	doc, err := index.BuildDocument(validator, &state)
	if err != nil {
		t.Fatalf("failed to build document: %v", err)
	}

	if diff := cmp.Diff(golden, doc.Fields); diff != "" {
		t.Errorf("DiscoverFields() mismatch (-want +got):\n%s", diff)
	}

	newMappings := doc.Mappings()

	mappingChanges := newMappings.ChangesFrom(baseMappings)

	if diff := cmp.Diff(goldenChanges, mappingChanges); diff != "" {
		t.Errorf("ChangesFrom() mismatch (-want +got):\n%s", diff)
	}

	superset := mappingChanges.Superset(baseMappings)

	if diff := cmp.Diff(goldenMappings, superset); diff != "" {
		t.Errorf("Superset() mismatch (-want +got):\n%s", diff)
	}
}

// TODO: When revisor doesn't depend on revisorschemas we can remove this and
// add the functionality to revisorschemas instead. Today that would create a
// dependency loop.
func decodeConstraintSetsFS(
	sFS embed.FS, names ...string,
) ([]revisor.ConstraintSet, error) {
	var constraints []revisor.ConstraintSet

	for _, n := range names {
		data, err := sFS.ReadFile(n)
		if err != nil {
			return nil, fmt.Errorf(
				"load constraints from %q: %w",
				n, err)
		}

		var c revisor.ConstraintSet

		err = decodeBytes(data, &c)
		if err != nil {
			return nil, fmt.Errorf(
				"parse constraints in %q: %w",
				n, err)
		}

		constraints = append(constraints, c)
	}

	return constraints, nil
}

func decodeBytes(data []byte, o interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	err := dec.Decode(o)
	if err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	return nil
}
