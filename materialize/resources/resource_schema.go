package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func Schema() *schema.Resource {
	return &schema.Resource{
		Description: "A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize’s secret management system.",

		CreateContext: resourceSchemaCreate,
		ReadContext:   resourceSchemaRead,
		UpdateContext: resourceSchemaUpdate,
		DeleteContext: resourceSchemaDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "The name of the schema.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"database_name": {
				Description: "The name of the database.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "materialize",
			},
		},
	}
}

type SchemaBuilder struct {
	schemaName   string
	databaseName string
}

func newSchemaBuilder(schemaName, databaseName string) *SchemaBuilder {
	return &SchemaBuilder{
		schemaName:   schemaName,
		databaseName: databaseName,
	}
}

func (b *SchemaBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SCHEMA %s.%s;`, b.databaseName, b.schemaName))
	return q.String()
}

func (b *SchemaBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`
		SELECT mz_schemas.id, mz_schemas.name, mz_databases.name
		FROM mz_schemas JOIN mz_databases
			ON mz_schemas.database_id = mz_databases.id
		WHERE mz_schemas.name = '%s'
		AND mz_databases.name = '%s';	
	`, b.schemaName, b.databaseName))
	return q.String()
}

func (b *SchemaBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP SCHEMA %s.%s;`, b.databaseName, b.schemaName))
	return q.String()
}

func resourceSchemaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*pgx.Conn)
	schemaName := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)

	builder := newSchemaBuilder(schemaName, databaseName)
	q := builder.Read()

	var id, name, database string
	conn.QueryRow(ctx, q).Scan(&id, &name, &database)

	d.SetId(id)
	d.Set("databaseName", database)
	d.Set("schemaName", name)

	return diags
}

func resourceSchemaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	schemaName := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)

	builder := newSchemaBuilder(schemaName, databaseName)
	q := builder.Create()

	Exec(ctx, conn, q)
	return resourceSchemaRead(ctx, d, meta)
}

func resourceSchemaUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceSchemaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	schemaName := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)

	builder := newSchemaBuilder(schemaName, databaseName)
	q := builder.Drop()

	return Exec(ctx, conn, q)
}
