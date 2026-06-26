#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
backend_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
cert_dir="$backend_dir/certs"
trusted_dir="$cert_dir/trusted"

mkdir -p "$cert_dir"
mkdir -p "$trusted_dir"

openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout "$cert_dir/ca.key" \
  -out "$cert_dir/ca.crt" \
  -subj "/CN=kurrentdb-dev-ca"

cat >"$cert_dir/node.conf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
keyUsage = keyEncipherment, dataEncipherment, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -newkey rsa:2048 -nodes \
  -keyout "$cert_dir/node.key" \
  -out "$cert_dir/node.csr" \
  -config "$cert_dir/node.conf"

openssl x509 -req -days 3650 \
  -in "$cert_dir/node.csr" \
  -CA "$cert_dir/ca.crt" \
  -CAkey "$cert_dir/ca.key" \
  -CAcreateserial \
  -out "$cert_dir/node.crt" \
  -extensions v3_req \
  -extfile "$cert_dir/node.conf"

chmod 600 "$cert_dir"/*.key
cp "$cert_dir/ca.crt" "$trusted_dir/ca.crt"

printf 'Generated dev certificates in %s\n' "$cert_dir"
