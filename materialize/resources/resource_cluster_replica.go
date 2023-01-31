package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/jackc/pgx/v4"
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
			"cluster_name": {
				Description: "The cluster whose resources you want to create an additional computation of.",
				Type:        schema.TypeString,
				Required:    true,
			},
			"replica_name": {
				Description: "A name for this replica.",
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
	clusterName                string
	replicaName                string
	size                       string
	availabilityZone           string
	introspectionInterval      string
	introspectionDebugging     bool
	idleArrangementMergeEffort int
}

func newClusterReplicaBuilder(clusterName, replicaName string) *ClusterReplicaBuilder {
	return &ClusterReplicaBuilder{
		clusterName: clusterName,
		replicaName: replicaName,
	}
}

func (crb *ClusterReplicaBuilder) Size(s string) *ClusterReplicaBuilder {
	crb.size = s
	return crb
}

func (crb *ClusterReplicaBuilder) AvailabilityZone(z string) *ClusterReplicaBuilder {
	crb.availabilityZone = z
	return crb
}

func (crb *ClusterReplicaBuilder) IntrospectionInterval(i string) *ClusterReplicaBuilder {
	crb.introspectionInterval = i
	return crb
}

func (crb *ClusterReplicaBuilder) IntrospectionDebugging() *ClusterReplicaBuilder {
	crb.introspectionDebugging = true
	return crb
}

func (crb *ClusterReplicaBuilder) IdleArrangementMergeEffort(e int) *ClusterReplicaBuilder {
	crb.idleArrangementMergeEffort = e
	return crb
}

func (crb *ClusterReplicaBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE CLUSTER REPLICA %s.%s`, crb.clusterName, crb.replicaName))

	if crb.size != "" {
		q.WriteString(fmt.Sprintf(` SIZE = '%s'`, crb.size))
	}

	if crb.availabilityZone != "" {
		q.WriteString(fmt.Sprintf(` AVAILABILITY ZONE = '%s'`, crb.availabilityZone))
	}

	if crb.introspectionInterval != "" {
		q.WriteString(fmt.Sprintf(` INTROSPECTION INTERVAL = '%s'`, crb.introspectionInterval))
	}

	if crb.introspectionDebugging {
		q.WriteString(` INTROSPECTION DEBUGGING = TRUE`)
	}

	if crb.idleArrangementMergeEffort != 0 {
		q.WriteString(fmt.Sprintf(` IDLE ARRANGEMENT MERGE EFFORT = %d`, crb.idleArrangementMergeEffort))
	}

	q.WriteString(`;`)
	return q.String()
}

func (crb *ClusterReplicaBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT * FROM mz_cluster_replicas WHERE id = %s;`, crb.replicaName))
	return q.String()
}

func (crb *ClusterReplicaBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP CLUSTER REPLICA %s.%s;`, crb.clusterName, crb.replicaName))
	return q.String()
}

func (crb *ClusterReplicaBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER CLUSTER REPLICA %s.%s TO %s;`, crb.clusterName, crb.replicaName, newName))
	return q.String()
}

func resourceClusterReplicaCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)
	replicaName := d.Get("replica_name").(string)

	builder := newClusterReplicaBuilder(clusterName, replicaName)

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

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName + ":" + replicaName)

	return diags
}

func resourceClusterReplicaRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)
	replicaName := d.Get("replica_name").(string)

	builder := newClusterReplicaBuilder(clusterName, replicaName)
	q := builder.Create()

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName + ":" + replicaName)

	return diags
}

func resourceClusterReplicaUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)
	replicaName := d.Get("replica_name").(string)

	if d.HasChange("name") {
		updatedName := d.Get("name").(string)

		builder := newClusterReplicaBuilder(clusterName, replicaName)
		q := builder.Rename(replicaName)

		Exec(ctx, conn, q)
		d.Set("name", updatedName)
	}

	return resourceClusterReplicaRead(ctx, d, meta)
}

func resourceClusterReplicaDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	clusterName := d.Get("cluster_name").(string)
	replicaName := d.Get("replica_name").(string)

	builder := newClusterReplicaBuilder(clusterName, replicaName)
	q := builder.Drop()

	diags := Exec(ctx, conn, q)
	d.SetId(clusterName + ":" + replicaName)

	return diags
}
