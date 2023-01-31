package resources

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResourceClusterCreate(t *testing.T) {
	r := require.New(t)
	b := newClusterBuilder("cluster")
	r.Equal(`CREATE CLUSTER cluster REPLICAS ();`, b.Create())
}

func TestResourceClusterRead(t *testing.T) {
	r := require.New(t)
	b := newClusterBuilder("cluster")
	r.Equal(`SELECT name FROM mz_clusters WHERE name = 'cluster';`, b.Read())
}

func TestResourceClusterDrop(t *testing.T) {
	r := require.New(t)
	b := newClusterBuilder("cluster")
	r.Equal(`DROP CLUSTER cluster;`, b.Drop())
}

func TestResourceClusterRename(t *testing.T) {
	r := require.New(t)
	b := newClusterBuilder("cluster")
	r.Equal(`ALTER CLUSTER cluster RENAME TO new_cluster;`, b.Rename("new_cluster"))
}
