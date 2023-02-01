package resources

import (
	"database/sql"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
)

func ExecResource(conn *sql.DB, queryStr string) diag.Diagnostics {
	var diags diag.Diagnostics

	_, execErr := conn.Exec(queryStr)

	if execErr != nil {
		return diag.FromErr(execErr)

	}

	return diags
}
