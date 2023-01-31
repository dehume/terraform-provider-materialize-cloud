resource "materialize_secret" "example" {
  name  = "example_secret"
  value = "decode('c2VjcmV0Cg==', 'base64')"
}