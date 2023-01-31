package datasources

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func DataSourceSecret() *schema.Resource {
	return &schema.Resource{
		ReadContext: resourceSecretRead,
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

func resourceSecretRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*pgx.Conn)

	secretName := d.Get("name").(string)
	queryStr := fmt.Sprintf("SELECT name FROM mz_secrets WHERE name = '%s';", secretName)

	var queriedName string
	conn.QueryRow(ctx, queryStr).Scan(&queriedName)

	d.SetId(queriedName)
	return diags
}
