package index_test

import (
	"log"
	"os"
	"testing"

	"github.com/ttab/eltest"
)

func TestMain(m *testing.M) {
	code := m.Run()

	err := eltest.PurgeBackingServices()
	if err != nil {
		log.Printf("failed to purge backing services: %v", err)
	}

	os.Exit(code)
}
