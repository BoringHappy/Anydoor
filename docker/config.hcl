ui            = true
disable_mlock = true
disable_cache = true

default_lease_ttl = "168h"
max_lease_ttl = "0h"

storage "file" {
  path = "/vault/data"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = "true"
}