package resources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

var replicaSizes = []string{
	"2xsmall",
	"xsmall",
	"small",
	"medium",
	"large",
	"xlarge",
	"x2large",
	"x3large",
	"x4large",
	"x5large",
	"x6large",
}

func ClusterReplica() *schema.Resource {
	return &schema.Resource{
		Description: "A logical cluster, which contains dataflow-powered objects.",

		CreateContext: resourceClusterReplicaCreate,
		ReadContext:   resourceClusterReplicaRead,
		UpdateContext: resourceClusterReplicaUpdate,
		DeleteContext: resourceClusterReplicaDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "A name for this replica.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"cluster_name": {
				Description: "The cluster whose resources you want to create an additional computation of.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"size": {
				Description:  "The size of the replica.",
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringInSlice(replicaSizes, true),
			},
			"availability_zone": {
				Description:  "If you want the replica to reside in a specific availability zone.",
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringInSlice([]string{"us-east-1", "eu-west-1"}, true),
			},
			"introspection_interval": {
				Description: "The interval at which to collect introspection data.",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "1s",
			},
			"introspection_debugging": {
				Description: "Whether to introspect the gathering of the introspection data.",
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
			},
			"idle_arrangement_merge_effort": {
				Description: "The amount of effort the replica should exert on compacting arrangements during idle periods.",
				Type:        schema.TypeInt,
				Optional:    true,
			},
		},
	}
}

type ClusterReplicaBuilder struct {
	replicaName                string
	clusterName                string
	size                       string
	availabilityZone           string
	introspectionInterval      string
	introspectionDebugging     bool
	idleArrangementMergeEffort int
}

func newClusterReplicaBuilder(clusterName, replicaName string) *ClusterReplicaBuilder {
	return &ClusterReplicaBuilder{
		replicaName: replicaName,
		clusterName: clusterName,
	}
}

func (b *ClusterReplicaBuilder) Size(s string) *ClusterReplicaBuilder {
	b.size = s
	return b
}

func (b *ClusterReplicaBuilder) AvailabilityZone(z string) *ClusterReplicaBuilder {
	b.availabilityZone = z
	return b
}

func (b *ClusterReplicaBuilder) IntrospectionInterval(i string) *ClusterReplicaBuilder {
	b.introspectionInterval = i
	return b
}

func (b *ClusterReplicaBuilder) IntrospectionDebugging() *ClusterReplicaBuilder {
	b.introspectionDebugging = true
	return b
}

func (b *ClusterReplicaBuilder) IdleArrangementMergeEffort(e int) *ClusterReplicaBuilder {
	b.idleArrangementMergeEffort = e
	return b
}

func (b *ClusterReplicaBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE CLUSTER REPLICA %s.%s`, b.clusterName, b.replicaName))

	if b.size != "" {
		q.WriteString(fmt.Sprintf(` SIZE = '%s'`, b.size))
	}

	if b.availabilityZone != "" {
		q.WriteString(fmt.Sprintf(` AVAILABILITY ZONE = '%s'`, b.availabilityZone))
	}

	if b.introspectionInterval != "" {
		q.WriteString(fmt.Sprintf(` INTROSPECTION INTERVAL = '%s'`, b.introspectionInterval))
	}

	if b.introspectionDebugging {
		q.WriteString(` INTROSPECTION DEBUGGING = TRUE`)
	}

	if b.idleArrangementMergeEffort != 0 {
		q.WriteString(fmt.Sprintf(` IDLE ARRANGEMENT MERGE EFFORT = %d`, b.idleArrangementMergeEffort))
	}

	q.WriteString(`;`)
	return q.String()
}

func (b *ClusterReplicaBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`
		SELECT
			mz_cluster_replicas.id,
			mz_cluster_replicas.name,
			mz_clusters.name,
			mz_cluster_replicas.size,
			mz_cluster_replicas.availability_zone
		FROM mz_cluster_replicas
		JOIN mz_clusters
			ON mz_cluster_replicas.cluster_id = mz_clusters.id
		WHERE mz_cluster_replicas.name = '%s'
		AND mz_clusters.name = '%s';
	`, b.replicaName, b.clusterName))
	return q.String()
}

func (b *ClusterReplicaBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP CLUSTER REPLICA %s.%s;`, b.clusterName, b.replicaName))
	return q.String()
}

func resourceClusterReplicaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*sql.DB)
	replicaName := d.Get("name").(string)
	clusterName := d.Get("cluster_name").(string)

	builder := newClusterReplicaBuilder(replicaName, clusterName)
	q := builder.Read()

	var id, name, cluster, size, availability_zone string
	conn.QueryRow(q).Scan(&id, &name, &cluster, &size, &availability_zone)

	d.SetId(id)
	d.Set("replicaName", name)
	d.Set("clusterName", cluster)
	d.Set("size", size)
	d.Set("availabilityZone", availability_zone)

	return diags
}

func resourceClusterReplicaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*sql.DB)

	replicaName := d.Get("name").(string)
	clusterName := d.Get("cluster_name").(string)

	builder := newClusterReplicaBuilder(replicaName, clusterName)

	// Set optionals
	if v, ok := d.GetOk("size"); ok {
		builder.Size(v.(string))
	}

	if v, ok := d.GetOk("availabilityZone"); ok {
		builder.AvailabilityZone(v.(string))
	}

	if v, ok := d.GetOk("introspectionInterval"); ok {
		builder.AvailabilityZone(v.(string))
	}

	if v, ok := d.GetOk("introspectionDebugging"); ok && v.(bool) {
		builder.IntrospectionDebugging()
	}

	if v, ok := d.GetOk("idleArrangementMergeEffort"); ok {
		builder.IdleArrangementMergeEffort(v.(int))
	}

	q := builder.Create()

	conn.Exec(q)
	return resourceClusterReplicaRead(ctx, d, meta)
}

func resourceClusterReplicaUpdate(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {
	return diag.Errorf("not implemented")
}

func resourceClusterReplicaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var diags diag.Diagnostics

	conn := meta.(*sql.DB)
	replicaName := d.Get("name").(string)
	clusterName := d.Get("cluster_name").(string)

	builder := newClusterReplicaBuilder(replicaName, clusterName)
	q := builder.Drop()

	conn.Exec(q)
	return diags
}
