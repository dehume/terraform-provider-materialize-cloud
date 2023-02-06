package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func Sink() *schema.Resource {
	return &schema.Resource{
		Description: "A connects Materialize to an external system you want to write data to, and provides details about how to encode that data.",

		CreateContext: resourceSinkCreate,
		ReadContext:   resourceSinkRead,
		UpdateContext: resourceSinkUpdate,
		DeleteContext: resourceSinkDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "The identifier for the secret.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"schema_name": {
				Description: "The identifier for the secret schema.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "public",
			},
			"cluster_name": {
				Description:   "The cluster to maintain this sink. If not specified, the size option must be specified.",
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"size"},
			},
			"size": {
				Description:   "The size of the sink.",
				Type:          schema.TypeString,
				Optional:      true,
				ForceNew:      true,
				ValidateFunc:  validation.StringInSlice(sourceSizes, true),
				ConflictsWith: []string{"cluster_name"},
			},
			"item_name": {
				Description:  "The name of the source, table or materialized view you want to send to the sink.",
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				RequiredWith: []string{"topic"},
			},
			// Broker
			"kafka_connection": {
				Description:  "The name of the Kafka connection to use in the source.",
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				RequiredWith: []string{"topic"},
			},
			"topic": {
				Description:  "The Kafka topic you want to subscribe to.",
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				RequiredWith: []string{"kafka_connection"},
			},
			"format": {
				Description: "How to decode raw bytes from different formats into data structures it can understand at runtime",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"envelope": {
				Description:  "How to interpret records (e.g. Append Only, Upsert).",
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(envelopes, true),
			},
			"schema_registry_connection": {
				Description: "The name of the connection to use for the shcema registry.",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
		},
	}
}

type SinkBuilder struct {
	sinkName                 string
	schemaName               string
	clusterName              string
	size                     string
	itemName                 string
	kafkaConnection          string
	topic                    string
	format                   string
	envelope                 string
	schemaRegistryConnection string
}

func newSinkBuilder(sinkName, schemaName string) *SinkBuilder {
	return &SinkBuilder{
		sinkName:   sinkName,
		schemaName: schemaName,
	}
}

func (b *SinkBuilder) ClusterName(c string) *SinkBuilder {
	b.clusterName = c
	return b
}

func (b *SinkBuilder) Size(s string) *SinkBuilder {
	b.size = s
	return b
}

func (b *SinkBuilder) ItemName(i string) *SinkBuilder {
	b.itemName = i
	return b
}

func (b *SinkBuilder) KafkaConnection(k string) *SinkBuilder {
	b.kafkaConnection = k
	return b
}

func (b *SinkBuilder) Topic(t string) *SinkBuilder {
	b.topic = t
	return b
}

func (b *SinkBuilder) Format(f string) *SinkBuilder {
	b.format = f
	return b
}

func (b *SinkBuilder) Envelope(e string) *SinkBuilder {
	b.envelope = e
	return b
}

func (b *SinkBuilder) SchemaRegistryConnection(s string) *SinkBuilder {
	b.schemaRegistryConnection = s
	return b
}

func (b *SinkBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SINK %s.%s FROM %s`, b.schemaName, b.sinkName, b.itemName))

	// Broker
	if b.kafkaConnection != "" {
		q.WriteString(fmt.Sprintf(` INTO KAFKA CONNECTION %s`, b.kafkaConnection))
	}

	if b.topic != "" {
		q.WriteString(fmt.Sprintf(` (TOPIC '%s')`, b.topic))
	}

	if b.format != "" {
		q.WriteString(fmt.Sprintf(` FORMAT %s`, b.format))
	}

	if b.schemaRegistryConnection != "" {
		q.WriteString(fmt.Sprintf(` USING CONFLUENT SCHEMA REGISTRY CONNECTION %s`, b.schemaRegistryConnection))
	}

	if b.envelope != "" {
		q.WriteString(fmt.Sprintf(` ENVELOPE %s`, b.envelope))
	}

	if b.size != "" {
		q.WriteString(fmt.Sprintf(` WITH (SIZE = '%s')`, b.size))
	} else if b.clusterName != "" {
		q.WriteString(fmt.Sprintf(` IN CLUSTER %s`, b.clusterName))
	} else {
		panic(`Must include either size or cluster`)
	}

	q.WriteString(`;`)
	return q.String()
}

func (b *SinkBuilder) Read() string {
	return fmt.Sprintf(`
		SELECT
			mz_sinks.id,
			mz_sinks.name,
			mz_sinks.type,
			mz_sinks.size,
			mz_sinks.envelope_type,
			mz_connections.name as connection_name,
			mz_clusters.name as cluster_name
		FROM mz_sinks
		JOIN mz_schemas
			ON mz_sinks.schema_id = mz_schemas.id
		LEFT JOIN mz_connections
			ON mz_sinks.connection_id = mz_connections.id
		LEFT JOIN mz_clusters
			ON mz_sinks.cluster_id = mz_clusters.id
		WHERE mz_sinks.name = '%s'
		AND mz_schemas.name = '%s';
	`, b.sinkName, b.schemaName)
}

func (b *SinkBuilder) Rename(newName string) string {
	return fmt.Sprintf(`ALTER SINK %s.%s RENAME TO %s.%s;`, b.schemaName, b.sinkName, b.schemaName, newName)
}

func (b *SinkBuilder) UpdateSize(newSize string) string {
	return fmt.Sprintf(`ALTER SINK %s.%s SET (SIZE = '%s');`, b.schemaName, b.sinkName, newSize)
}

func (b *SinkBuilder) Drop() string {
	return fmt.Sprintf(`DROP SINK %s.%s;`, b.schemaName, b.sinkName)
}

func resourceSinkCreate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceSinkRead(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceSinkUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceSinkDelete(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}
