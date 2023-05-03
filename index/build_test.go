package index_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/revisor/constraints"
	"github.com/ttab/elephantine"
)

func TestBuildDocument(t *testing.T) {
	var (
		state            index.DocumentState
		baseMappings     index.Mappings
		golden           map[string]index.Field
		extraConstraints revisor.ConstraintSet
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

	coreConstraints, err := constraints.CoreSchema()
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	err = elephantine.UnmarshalFile(
		"testdata/tt.json", &extraConstraints)
	if err != nil {
		t.Fatalf("failed to load org constraints: %v", err)
	}

	validator, err := revisor.NewValidator(coreConstraints, extraConstraints)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	doc := index.BuildDocument(validator, &state)

	if diff := cmp.Diff(golden, doc.Fields); diff != "" {
		t.Errorf("DiscoverFields() mismatch (-want +got):\n%s", diff)
	}
}
