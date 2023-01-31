package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jackc/pgx/v4"
)

func ConnectionAWSPrivatelink() *schema.Resource {
	return &schema.Resource{
		Description: "A logical cluster, which contains dataflow-powered objects.",

		CreateContext: resourceConnectionAWSPrivatelinkCreate,
		ReadContext:   resourceConnectionAWSPrivatelinkRead,
		UpdateContext: resourceConnectionAWSPrivatelinkUpdate,
		DeleteContext: resourceConnectionAWSPrivatelinkDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "The name of the connection.",
				Type:        schema.TypeString,
				Required:    true,
			},
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

type ConnectionAWSPrivatelinkBuilder struct {
	connectionName    string
	serviceName       string
	availabilityZones []string
}

func newConnectionAWSPrivatelinkBuilder(connectionName string) *ConnectionAWSPrivatelinkBuilder {
	return &ConnectionAWSPrivatelinkBuilder{
		connectionName: connectionName,
	}
}

func (b *ConnectionAWSPrivatelinkBuilder) ServiceName(s string) *ConnectionAWSPrivatelinkBuilder {
	b.serviceName = s
	return b
}

func (b *ConnectionAWSPrivatelinkBuilder) AvailabilityZones(a []string) *ConnectionAWSPrivatelinkBuilder {
	b.availabilityZones = a
	return b
}

func (b *ConnectionAWSPrivatelinkBuilder) Create() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE CONNECTION %s TO PRIVATELINK`, b.connectionName))

	if b.serviceName != "" {
		q.WriteString(fmt.Sprintf(` SERVICE NAME = '%s'`, b.serviceName))
	}

	if len(b.availabilityZones) > 0 {
		q.WriteString(fmt.Sprintf(` AVAILABILITY ZONES ('%v')`, b.availabilityZones))
	}

	q.WriteString(`;`)
	return q.String()
}

func (b *ConnectionAWSPrivatelinkBuilder) Read() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`SELECT name FROM mz_connections WHERE name = %s;`, b.connectionName))
	return q.String()
}

func (b *ConnectionAWSPrivatelinkBuilder) Drop() string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`DROP CONNECTION %s;`, b.connectionName))
	return q.String()
}

func (b *ConnectionAWSPrivatelinkBuilder) Rename(newName string) string {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`ALTER CONNECTION %s RENAME TO %s;`, b.connectionName, newName))
	return q.String()
}

func resourceConnectionAWSPrivatelinkCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	connectionName := d.Get("name").(string)

	builder := newConnectionAWSPrivatelinkBuilder(connectionName)
	q := builder.Create()

	diags := Exec(ctx, conn, q)
	d.SetId(connectionName)

	return diags
}

func resourceConnectionAWSPrivatelinkRead(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	connectionName := d.Get("name").(string)

	builder := newConnectionAWSPrivatelinkBuilder(connectionName)
	q := builder.Read()

	diags := Exec(ctx, conn, q)
	d.SetId(connectionName)

	return diags
}

func resourceConnectionAWSPrivatelinkUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	connectionName := d.Get("name").(string)

	if d.HasChange("name") {
		updatedName := d.Get("name").(string)

		builder := newConnectionAWSPrivatelinkBuilder(connectionName)
		q := builder.Rename(updatedName)

		Exec(ctx, conn, q)
		d.Set("name", updatedName)
	}

	return resourceConnectionAWSPrivatelinkRead(ctx, d, meta)
}

func resourceConnectionAWSPrivatelinkDelete(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*pgx.Conn)

	connectionName := d.Get("name").(string)

	builder := newConnectionAWSPrivatelinkBuilder(connectionName)
	q := builder.Drop()

	diags := Exec(ctx, conn, q)
	d.SetId(connectionName)

	return diags
}
