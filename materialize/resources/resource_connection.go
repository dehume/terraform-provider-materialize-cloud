package resources

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ConnectionAWSPrivatelink() *schema.Resource {
	return &schema.Resource{
		Description: "A logical cluster, which contains dataflow-powered objects.",

		CreateContext: resourceConnectionAWSPrivatelinkCreate,
		ReadContext:   resourceConnectionAWSPrivatelinkRead,
		UpdateContext: resourceConnectionAWSPrivatelinkUpdate,
		DeleteContext: resourceConnectionAWSPrivatelinkDelete,

		Schema: map[string]*schema.Schema{
			"service_name": {
				Description: "The name of the AWS PrivateLink service.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"availability_zones": {
				Description: "The IDs of the AWS availability zones in which the service is accessible.",
				Type:        schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Required: true,
			},
		},
	}
}

func resourceConnectionAWSPrivatelinkCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceConnectionAWSPrivatelinkRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceConnectionAWSPrivatelinkUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceConnectionAWSPrivatelinkDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	return diag.Errorf("not implemented")
}
