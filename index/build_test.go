package index_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/newsdoc"
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

	regenerate := test.Regenerate()

	err := elephantine.UnmarshalFile(
		"testdata/raw_1.input.json", &state)
	if err != nil {
		t.Fatalf("failed to load state data: %v", err)
	}

	constraints, err := revisor.DecodeConstraintSetsFS(revisorschemas.Files(),
		"core.json", "core-planning.json", "tt.json")
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	validator, err := revisor.NewValidator(constraints...)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	lRes := index.NewLanguageResolver(index.LanguageOptions{
		DefaultLanguage: "sv-se",
	})

	lang, err := lRes.GetLanguageInfo("sv-se")
	test.Must(t, err, "get language configuration")

	langConf := index.GetIndexConfig(lang)

	doc, err := index.BuildDocument(validator, &state, langConf, map[string]bool{
		index.FeatureSortable: true,
		index.FeaturePrefix:   true,
		index.FeatureOnlyICU:  true,
	})
	if err != nil {
		t.Fatalf("failed to build document: %v", err)
	}

	if regenerate {
		err := elephantine.MarshalFile(
			"testdata/raw_1.fields.json", doc.Fields)
		test.Must(t, err, "update golden state")
	}

	err = elephantine.UnmarshalFile(
		"testdata/raw_1.fields.json", &golden)
	if err != nil {
		t.Fatalf("failed to load golden state: %v", err)
	}

	if diff := cmp.Diff(golden, doc.Fields); diff != "" {
		t.Errorf("DiscoverFields() mismatch (-want +got):\n%s", diff)
	}

	newMappings := doc.Mappings()

	err = elephantine.UnmarshalFile(
		"testdata/mapping_subset.json", &baseMappings)
	if err != nil {
		t.Fatalf("failed to load base mappings: %v", err)
	}

	mappingChanges := newMappings.ChangesFrom(baseMappings)

	if regenerate {
		err := elephantine.MarshalFile(
			"testdata/mapping_changes.json", mappingChanges)
		test.Must(t, err, "update golden mapping changes")
	}

	err = elephantine.UnmarshalFile(
		"testdata/mapping_changes.json", &goldenChanges)
	if err != nil {
		t.Fatalf("failed to load golden mapping changes: %v", err)
	}

	if diff := cmp.Diff(goldenChanges, mappingChanges); diff != "" {
		t.Errorf("ChangesFrom() mismatch (-want +got):\n%s", diff)
	}

	superset := mappingChanges.Superset(baseMappings)

	if regenerate {
		err := elephantine.MarshalFile(
			"testdata/mapping_superset.json", superset)
		test.Must(t, err, "update golden mapping superset")
	}

	err = elephantine.UnmarshalFile(
		"testdata/mapping_superset.json", &goldenMappings)
	if err != nil {
		t.Fatalf("failed to load golden mapping superset: %v", err)
	}

	if diff := cmp.Diff(goldenMappings, superset); diff != "" {
		t.Errorf("Superset() mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildDocumentFields(t *testing.T) {
	regenerate := test.Regenerate()

	constraints, err := revisor.DecodeConstraintSetsFS(revisorschemas.Files(),
		"core.json", "core-planning.json", "tt.json")
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	validator, err := revisor.NewValidator(constraints...)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	lRes := index.NewLanguageResolver(index.LanguageOptions{
		DefaultLanguage: "sv-se",
	})

	lang, err := lRes.GetLanguageInfo("sv-se")
	test.Must(t, err, "get language configuration")

	langConf := index.GetIndexConfig(lang)

	docFiles, err := filepath.Glob(filepath.Join("..", "testdata", "documents", "*.json"))
	test.Must(t, err, "glob for files")

	fixedTime := time.Date(2026, time.January, 0o2, 16, 13, 0, 0, time.UTC)

	testDir := filepath.Join("..", "testdata", t.Name())

	err = os.MkdirAll(testDir, 0o770)
	test.Must(t, err, "ensure testdata directory")

	for _, file := range docFiles {
		name := filepath.Base(file)

		var doc newsdoc.Document

		err := elephantine.UnmarshalFile(file, &doc)
		test.Must(t, err, "unmarshal source document")

		state := index.DocumentState{
			Created:        fixedTime,
			Creator:        "core://user/1",
			Modified:       fixedTime,
			Updater:        "core://user/1",
			CurrentVersion: 1,
			ACL: []index.ACLEntry{
				{
					URI:         "core://unit/redaktionen",
					Permissions: []string{"r", "w"},
				},
			},
			Heads:    map[string]index.Status{},
			Document: doc,
		}

		built, err := index.BuildDocument(validator, &state, langConf, map[string]bool{
			index.FeatureSortable: true,
			index.FeaturePrefix:   true,
			index.FeatureOnlyICU:  true,
		})
		test.Must(t, err, "build document")

		test.TestAgainstGolden(t, regenerate, built.Fields,
			filepath.Join(testDir, name))
	}
}
