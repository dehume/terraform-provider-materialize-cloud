package resources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceSourceCreate(t *testing.T) {
	r := require.New(t)

	bs := newSourceBuilder("source", "schema")
	bs.Size("xsmall")
	r.Equal(`CREATE SOURCE schema.source WITH (SIZE = 'xsmall');`, bs.Create())

	bc := newSourceBuilder("source", "schema")
	bc.ClusterName("cluster")
	r.Equal(`CREATE SOURCE schema.source IN CLUSTER cluster;`, bc.Create())

	// Connection Variations
	bl := newSourceBuilder("source", "schema")
	bl.Size("xsmall").ConnectionType("LOAD GENERATOR").LoadGeneratorType("TPCH").TickInterval("1s").ScaleFactor(0.01)
	r.Equal(`CREATE SOURCE schema.source FROM LOAD GENERATOR TPCH (TICK INTERVAL '1s', SCALE FACTOR 0.01) WITH (SIZE = 'xsmall');`, bl.Create())

	bt := newSourceBuilder("source", "schema")
	tables := map[string]string{
		"schema1.table_1": "s1_table_1",
		"schema2_table_1": "s2_table_1",
	}
	bt.Size("xsmall").ConnectionType("POSTGRES").PostgresConnection("pg_connection").Publication("mz_source")
	r.Equal(`CREATE SOURCE schema.source FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source') FOR ALL TABLES WITH (SIZE = 'xsmall');`, bt.Create())

	bp := newSourceBuilder("source", "schema")
	bp.Size("xsmall").ConnectionType("POSTGRES").PostgresConnection("pg_connection").Publication("mz_source").Tables(tables)
	r.Equal(`CREATE SOURCE schema.source FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source') FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1) WITH (SIZE = 'xsmall');`, bp.Create())

	bb := newSourceBuilder("source", "schema")
	bb.Size("xsmall").ConnectionType("KAFKA").KafkaConnection("kafka_connection").Topic("events").Format("AVRO").SchemaRegistryConnection("csr_connection").Envelope("UPSERT")
	r.Equal(`CREATE SOURCE schema.source FROM KAFKA CONNECTION kafka_connection (TOPIC 'events') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection ENVELOPE UPSERT WITH (SIZE = 'xsmall');`, bb.Create())
}

func TestResourceSourceRead(t *testing.T) {
	r := require.New(t)
	b := newSourceBuilder("source", "schema")
	r.Equal(`
		SELECT
			mz_sources.id,
			mz_sources.name,
			mz_sources.type,
			mz_sources.size,
			mz_sources.envelope_type,
			mz_connections.name as connection_name,
			mz_clusters.name as cluster_name
		FROM mz_sources
		JOIN mz_schemas
			ON mz_sources.schema_id = mz_schemas.id
		LEFT JOIN mz_connections
			ON mz_sources.connection_id = mz_connections.id
		LEFT JOIN mz_clusters
			ON mz_sources.cluster_id = mz_clusters.id
		WHERE mz_sources.name = 'source'
		AND mz_schemas.name = 'schema';
	`, b.Read())
}

func TestResourceSourceRename(t *testing.T) {
	r := require.New(t)
	b := newSourceBuilder("source", "schema")
	r.Equal(`ALTER SOURCE schema.source RENAME TO schema.new_source;`, b.Rename("new_source"))
}

func TestResourceSourceResize(t *testing.T) {
	r := require.New(t)
	b := newSourceBuilder("source", "schema")
	r.Equal(`ALTER SOURCE schema.source SET (SIZE = 'xlarge');`, b.UpdateSize("xlarge"))
}

func TestResourceSourceDrop(t *testing.T) {
	r := require.New(t)
	b := newSourceBuilder("source", "schema")
	r.Equal(`DROP SOURCE schema.source;`, b.Drop())
}
