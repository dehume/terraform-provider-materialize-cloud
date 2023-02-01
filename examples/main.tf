terraform {
  required_providers {
    materialize = {
      version = "0.1.0"
      source  = "materialize.com/devex/materialize"
    }
  }
}

provider "materialize" {
  host     = local.host
  username = local.username
  password = local.password
  port     = local.port
  database = local.database
}

# resource "materialize_secret" "example_secret" {
#   name  = "example_dsh"
#   value = "decode('c2VjcmV0Cg==X', 'base64')"
# }

# resource "materialize_database" "example_database" {
#   name = "example_dsh"
# }

# resource "materialize_schema" "example_schema" {
#   name          = "example_dsh"
#   database_name = materialize_database.example_database.name
# }

# resource "materialize_cluster" "example_cluster" {
#   name = "example_dsh"
# }

# resource "materialize_cluster_replica" "example_cluster_replica" {
#   name         = "example_dsh"
#   cluster_name = materialize_cluster.example_cluster.name
#   size         = "2xsmall"
# }