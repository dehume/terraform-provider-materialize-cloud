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
				Required:    true,
			},
			"schema_name": {
				Description: "The identifier for the secret schema.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "public",
			},
			"value": {
				Description: "The value for the secret. The value expression may not reference any relations, and must be implicitly castable to bytea.",
				Type:        schema.TypeString,
				Required:    true,
				Sensitive:   true,
			},
		},
	}
}

type SecretBuilder struct {
	secretName string
	schemaName string
}

func newSecretBuilder(secretName, schemaName string) *SecretBuilder {
	return &SecretBuilder{
		secretName: secretName,
		schemaName: schemaName,
	}
}

func (b *SecretBuilder) Create(value string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE SECRET %s.%s AS %s;`, b.schemaName, b.secretName, value))
	return q.String()
}

func (b *SecretBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`
		SELECT mz_secrets.id, mz_secrets.name, mz_schemas.name
		FROM mz_secrets JOIN mz_schemas
			ON mz_secrets.schema_id = mz_schemas.id
		WHERE mz_secrets.name = '%s'
		AND mz_schemas.name = '%s';
	`, b.secretName, b.schemaName))
	return q.String()
}

func (b *SecretBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER SECRET %s.%s RENAME TO %s.%s;`, b.schemaName, b.secretName, b.schemaName, newName))
	return q.String()
}

func (b *SecretBuilder) UpdateValue(newValue string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER SECRET %s.%s AS %s;`, b.schemaName, b.secretName, newValue))
	return q.String()
}

func (b *SecretBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP SECRET %s.%s;`, b.schemaName, b.secretName))
	return q.String()
}

func resourceSecretRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)
	schemaName := d.Get("name").(string)

	builder := newSecretBuilder(secretName, schemaName)
	q := builder.Read()

	var id, name, schema string
	conn.QueryRow(ctx, q).Scan(&id, &name, &schema)

	d.SetId(id)
	d.Set("schemaName", schema)
	d.Set("secretName", name)

	return diags
}

func resourceSecretCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)
	schemaName := d.Get("name").(string)
	value := d.Get("value").(string)

	builder := newSecretBuilder(secretName, schemaName)
	q := builder.Create(value)

	Exec(ctx, conn, q)
	return resourceSecretRead(ctx, d, meta)
}

func resourceSecretUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	schemaName := d.Get("name").(string)

	if d.HasChange("name") {
		oldName, newName := d.GetChange("name")

		builder := newSecretBuilder(oldName.(string), schemaName)
		q := builder.Rename(newName.(string))

		Exec(ctx, conn, q)
		d.Set("secretName", newName)
	}

	if d.HasChange("value") {
		oldValue, newValue := d.GetChange("value")

		builder := newSecretBuilder(oldValue.(string), schemaName)
		q := builder.UpdateValue(newValue.(string))

		Exec(ctx, conn, q)
		d.Set("value", newValue)
	}

	return resourceSecretRead(ctx, d, meta)
}

func resourceSecretDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)
	secretName := d.Get("name").(string)
	schemaName := d.Get("name").(string)

	builder := newSecretBuilder(secretName, schemaName)
	q := builder.Drop()

	return Exec(ctx, conn, q)
}
