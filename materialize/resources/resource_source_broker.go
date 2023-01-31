package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func SourceBroker() *schema.Resource {
	return &schema.Resource{
		Description: "A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize’s secret management system.",

		CreateContext: resourceSourceBrokerCreate,
		ReadContext:   resourceSourceBrokerRead,
		UpdateContext: resourceSourceBrokerUpdate,
		DeleteContext: resourceSourceBrokerDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "The identifier for the secret.",
				Type:        schema.TypeString,
				Optional:    true,
			},
			"value": {
				Description: "The value for the secret. The value expression may not reference any relations, and must be implicitly castable to bytea.",
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
			},
		},
	}
}

type SourceBrokerBuilder struct {
	sourceName string
}

func newSourceBrokerBuilder(sourceName string) *SourceBrokerBuilder {
	return &SourceBrokerBuilder{
		sourceName: sourceName,
	}
}

func (b *SourceBrokerBuilder) Create(value string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SECRET %s AS %s;`, b.sourceName, value))
	return q.String()
}

func (b *SourceBrokerBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT name FROM mz_secrets WHERE name = '%s';`, b.sourceName))
	return q.String()
}

func (b *SourceBrokerBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER SECRET %s RENAME TO %s;`, b.sourceName, newName))
	return q.String()
}

func (b *SourceBrokerBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP SECRET %s;`, b.sourceName))
	return q.String()
}

func resourceSourceBrokerCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	sourceName := d.Get("name").(string)
	value := d.Get("value").(string)

	builder := newSourceBrokerBuilder(sourceName)
	q := builder.Create(value)

	diags := Exec(ctx, conn, q)
	d.SetId(sourceName)

	return diags
}

func resourceSourceBrokerRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*pgx.Conn)
	sourceName := d.Get("name").(string)

	builder := newSourceBrokerBuilder(sourceName)
	q := builder.Read()

	var n string
	conn.QueryRow(ctx, q).Scan(&n)

	d.SetId(n)
	return diags
}

func resourceSourceBrokerUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	sourceName := d.Id()

	if d.HasChange("name") {
		updatedName := d.Get("name").(string)

		builder := newSourceBrokerBuilder(sourceName)
		q := builder.Rename(updatedName)

		Exec(ctx, conn, q)
		d.Set("name", updatedName)
	}

	return resourceSourceBrokerRead(ctx, d, meta)
}

func resourceSourceBrokerDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	sourceName := d.Get("name").(string)

	builder := newSourceBrokerBuilder(sourceName)
	q := builder.Drop()

	return Exec(ctx, conn, q)
}
