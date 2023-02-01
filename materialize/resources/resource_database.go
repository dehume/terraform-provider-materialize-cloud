package resources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Database() *schema.Resource {
	return &schema.Resource{
		Description: "A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize’s secret management system.",

		CreateContext: resourceDatabaseCreate,
		ReadContext:   resourceDatabaseRead,
		UpdateContext: resourceDatabaseUpdate,
		DeleteContext: resourceDatabaseDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "The identifier for the database.",
				Type:        schema.TypeString,
				Optional:    true,
			},
		},
	}
}

type DatabaseBuilder struct {
	databaseName string
}

func newDatabaseBuilder(databaseName string) *DatabaseBuilder {
	return &DatabaseBuilder{
		databaseName: databaseName,
	}
}

func (b *DatabaseBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE DATABASE %s;`, b.databaseName))
	return q.String()
}

func (b *DatabaseBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT id, name FROM mz_databases WHERE name = '%s';`, b.databaseName))
	return q.String()
}

func (b *DatabaseBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP DATABASE %s;`, b.databaseName))
	return q.String()
}

func resourceDatabaseRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*sql.DB)
	databaseName := d.Get("name").(string)

	builder := newDatabaseBuilder(databaseName)
	q := builder.Read()

	var id, name string
	conn.QueryRow(q).Scan(&id, &name)

	d.SetId(id)
	d.Set("databaseName", name)

	return diags
}

func resourceDatabaseCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*sql.DB)
	databaseName := d.Get("name").(string)

	builder := newDatabaseBuilder(databaseName)
	q := builder.Create()

	Exec(ctx, conn, q)
	return resourceDatabaseRead(ctx, d, meta)
}

func resourceDatabaseUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceDatabaseDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*sql.DB)
	databaseName := d.Get("name").(string)

	builder := newDatabaseBuilder(databaseName)
	q := builder.Drop()

	return Exec(ctx, conn, q)
}
