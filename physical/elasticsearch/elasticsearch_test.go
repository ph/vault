package elasticsearch

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/vault/helper/logformat"
	"github.com/hashicorp/vault/physical"
	log "github.com/mgutz/logxi/v1"
)

func TestElasticsearchBackend(t *testing.T) {
	host := os.Getenv("ELASTICSEARCH_HOST")
	index := os.Getenv("ELASTICSEARCH_INDEX")

	if host == "" || index == "" {
		t.SkipNow()
	}

	logger := logformat.NewVaultLogger(log.LevelTrace)
	b, err := NewElasticsearchBackend(map[string]string{
		"host":        host,
		"tls_disable": "1",
	}, logger)

	if err != nil {
		t.Fatalf("Failed to create a new backend: %v", err)
	}

	defer func() {
		client, err := NewElasticsearchClient([]string{host}, nil, logger)
		if err != nil {
			t.Fatalf("Failed to create the elasticsearch client: %v", err)
		}

		_, err = client.Delete(fmt.Sprintf("_template/%s", index), nil)
		if err != nil {
			t.Fatalf("Failed to delete template: %v", err)
		}

		_, err = client.Delete(fmt.Sprintf("%s", index), nil)
		if err != nil {
			t.Fatalf("Failed to delete indices: %v", err)
		}
	}()

	physical.ExerciseBackend(t, b)
	physical.ExerciseBackend_ListPrefix(t, b)
}
