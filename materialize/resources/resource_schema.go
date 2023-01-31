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
				Optional:    true,
			},
		},
	}
}

type SchemaBuilder struct {
	schemaName string
}

func newSchemaBuilder(schemaName string) *SchemaBuilder {
	return &SchemaBuilder{
		schemaName: schemaName,
	}
}

func (b *SchemaBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SCHEMA %s;`, b.schemaName))
	return q.String()
}

func (b *SchemaBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT name FROM mz_schemas WHERE name = '%s';`, b.schemaName))
	return q.String()
}

func (b *SchemaBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP SCHEMA %s;`, b.schemaName))
	return q.String()
}

func resourceSchemaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	schemaName := d.Get("name").(string)

	builder := newSchemaBuilder(schemaName)
	q := builder.Create()

	diags := Exec(ctx, conn, q)
	d.SetId(schemaName)

	return diags
}

func resourceSchemaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	schemaName := d.Get("name").(string)

	builder := newSchemaBuilder(schemaName)
	q := builder.Read()

	diags := Exec(ctx, conn, q)
	d.SetId(schemaName)

	return diags
}

func resourceSchemaUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceSchemaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	schemaName := d.Get("name").(string)

	builder := newSchemaBuilder(schemaName)
	q := builder.Drop()

	diags := Exec(ctx, conn, q)
	d.SetId(schemaName)

	return diags
}
