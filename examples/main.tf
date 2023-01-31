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

resource "materialize_secret" "example" {
  name  = "example_dsh"
  value = "decode('c2VjcmV0Cg==', 'base64')"
}