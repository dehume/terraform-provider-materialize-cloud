package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func Secret() *schema.Resource {
	return &schema.Resource{
		Description: "A secret securely stores sensitive credentials (like passwords and SSL keys) in Materialize’s secret management system.",

		CreateContext: resourceSecretCreate,
		ReadContext:   resourceSecretRead,
		UpdateContext: resourceSecretUpdate,
		DeleteContext: resourceSecretDelete,

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

type SecretBuilder struct {
	secretName string
}

func newSecretBuilder(secretName string) *SecretBuilder {
	return &SecretBuilder{
		secretName: secretName,
	}
}

func (sb *SecretBuilder) Create(value string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SECRET %s AS %s;`, sb.secretName, value))
	return q.String()
}

func (sb *SecretBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT name FROM mz_secrets WHERE name = '%s';`, sb.secretName))
	return q.String()
}

func (sb *SecretBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER SECRET %s RENAME TO %s;`, sb.secretName, newName))
	return q.String()
}

func (sb *SecretBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP SECRET %s;`, sb.secretName))
	return q.String()
}

func resourceSecretCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)
	value := d.Get("value").(string)

	builder := newSecretBuilder(secretName)
	q := builder.Create(value)

	diags := Exec(ctx, conn, q)
	d.SetId(secretName)

	return diags
}

func resourceSecretRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)

	builder := newSecretBuilder(secretName)
	q := builder.Read()

	var n string
	conn.QueryRow(ctx, q).Scan(&n)

	d.SetId(n)
	return diags
}

func resourceSecretUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	secretName := d.Id()

	if d.HasChange("name") {
		updatedName := d.Get("name").(string)

		builder := newSecretBuilder(secretName)
		q := builder.Rename(updatedName)

		Exec(ctx, conn, q)
		d.Set("name", updatedName)
	}

	return resourceSecretRead(ctx, d, meta)
}

func resourceSecretDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)

	builder := newSecretBuilder(secretName)
	q := builder.Drop()

	return Exec(ctx, conn, q)
}
