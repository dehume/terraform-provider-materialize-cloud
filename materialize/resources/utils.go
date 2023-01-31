package resources

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/jackc/pgx/v4"
)

func Exec(ctx context.Context, conn *pgx.Conn, queryStr string) diag.Diagnostics {
	var diags diag.Diagnostics

	_, err := conn.Exec(ctx, queryStr)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}
