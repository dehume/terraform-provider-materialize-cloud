package resources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceSchemaCreate(t *testing.T) {
	r := require.New(t)
	b := newSchemaBuilder("schema")
	r.Equal(`CREATE SCHEMA schema;`, b.Create())
}

func TestResourceSchemaRead(t *testing.T) {
	r := require.New(t)
	b := newSchemaBuilder("schema")
	r.Equal(`SELECT name FROM mz_schemas WHERE name = 'schema';`, b.Read())
}

func TestResourceSchemaDrop(t *testing.T) {
	r := require.New(t)
	b := newSchemaBuilder("schema")
	r.Equal(`DROP SCHEMA schema;`, b.Drop())
}
