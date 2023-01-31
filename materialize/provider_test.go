package provider

import (
	"testing"
)

func TestConnectionString(t *testing.T) {
	msg := connectionString("host", "user", "pass", 6875, "database")
	if msg != "postgres://user:pass@host:6875/database?sslmode=require" {
		t.Fatalf("Incorrect connection string")
	}
}
