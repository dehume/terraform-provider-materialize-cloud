package resources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func Cluster() *schema.Resource {
	return &schema.Resource{
		Description: "A logical cluster, which contains dataflow-powered objects.",

		CreateContext: resourceClusterCreate,
		ReadContext:   resourceClusterRead,
		UpdateContext: resourceClusterUpdate,
		DeleteContext: resourceClusterDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "A name for the cluster.",
				Type:        schema.TypeString,
				Required:    true,
			},
		},
	}
}

type ClusterBuilder struct {
	clusterName string
}

func newClusterBuilder(clusterName string) *ClusterBuilder {
	return &ClusterBuilder{
		clusterName: clusterName,
	}
}

func (b *ClusterBuilder) Create() string {
	q := strings.Builder{}
	// Only create empty clusters, manage replicas with separate resource
	q.WriteString(fmt.Sprintf(`CREATE CLUSTER %s REPLICAS ();`, b.clusterName))
	return q.String()
}

func (b *ClusterBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT id, name FROM mz_clusters WHERE name = '%s';`, b.clusterName))
	return q.String()
}

func (b *ClusterBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP CLUSTER %s;`, b.clusterName))
	return q.String()
}

func resourceClusterRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*sql.DB)
	clusterName := d.Get("name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Read()

	var id, name string
	conn.QueryRow(q).Scan(&id, &name)

	d.SetId(id)
	d.Set("clusterName", name)

	return diags
}

func resourceClusterCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*sql.DB)
	clusterName := d.Get("name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Create()

	ExecResource(conn, q)
	return resourceClusterRead(ctx, d, meta)
}

func resourceClusterUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceClusterDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*sql.DB)
	clusterName := d.Get("name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Drop()

	ExecResource(conn, q)
	return diags
}
