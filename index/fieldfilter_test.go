package index_test

import (
	"testing"

	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine/test"
)

// TestFieldFilter pins the subscription field filter's matching semantics.
// The filter is the only place the service uses gobwas/glob directly, and
// the separator argument is what keeps a "*" from crossing a dot in a field
// path, so the behaviour is worth stating rather than inheriting from
// whichever matching engine the library ships.
func TestFieldFilter(t *testing.T) {
	cases := map[string]map[string]bool{
		"document.title": {
			"document.title":      true,
			"document.title.sort": false,
			"document.titles":     false,
			"heads.usable.title":  false,
		},
		"document.*": {
			"document.title":                   true,
			"document.uri":                     true,
			"document.meta.core_note.data.tex": false,
			"document":                         false,
			"heads.usable.version":             false,
		},
		"document.**": {
			"document.title":                    true,
			"document.meta.core_note.data.text": true,
			"document":                          false,
		},
		"document.rel.*.uuid": {
			"document.rel.subject.uuid":     true,
			"document.rel.section.uuid":     true,
			"document.rel.subject.uri":      false,
			"document.rel.deep.nested.uuid": false,
		},
	}

	for pattern, probes := range cases {
		t.Run(pattern, func(t *testing.T) {
			ff, err := index.NewFieldFilter([]string{pattern})
			test.Mustf(t, err, "compile field filter %q", pattern)

			for field, want := range probes {
				test.Equalf(t, want, ff.Includes(field),
					"match %q against %q", field, pattern)
			}
		})
	}
}

func TestFieldFilterExactAndGlob(t *testing.T) {
	ff, err := index.NewFieldFilter([]string{
		"document.uuid",
		"document.meta.**",
	})
	test.Mustf(t, err, "compile field filter")

	probes := map[string]bool{
		"document.uuid":                     true,
		"document.meta.core_note.data.text": true,
		"document.title":                    false,
	}

	for field, want := range probes {
		test.Equalf(t, want, ff.Includes(field), "match %q", field)
	}
}

func TestFieldFilterInvalidPattern(t *testing.T) {
	// Only an expression containing "*" is compiled as a glob; everything
	// else is taken as an exact field name, however odd it looks.
	_, err := index.NewFieldFilter([]string{"document.*{a"})
	test.MustNotf(t, err, "reject an unparseable field expression")

	_, err = index.NewFieldFilter([]string{"document.[a-"})
	test.Mustf(t, err, "accept a glob-free expression as an exact name")
}
