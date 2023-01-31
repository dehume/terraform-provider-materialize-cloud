package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
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
	q.WriteString(fmt.Sprintf(`SELECT name FROM mz_clusters WHERE name = '%s';`, b.clusterName))
	return q.String()
}

func (b *ClusterBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP CLUSTER %s;`, b.clusterName))
	return q.String()
}

func (b *ClusterBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER CLUSTER %s RENAME TO %s;`, b.clusterName, newName))
	return q.String()
}

func resourceClusterCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Create()

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName)

	return diags
}

func resourceClusterRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Read()

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName)

	return diags
}

func resourceClusterUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)

	if d.HasChange("name") {
		updatedName := d.Get("name").(string)

		builder := newClusterBuilder(clusterName)
		q := builder.Rename(updatedName)

		Exec(ctx, conn, q)
		d.Set("name", updatedName)
	}

	return resourceClusterRead(ctx, d, meta)
}

func resourceClusterDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)

	builder := newClusterBuilder(clusterName)
	q := builder.Drop()

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName)

	return diags
}
