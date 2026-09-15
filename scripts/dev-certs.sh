#!/bin/sh
# Generates a throwaway CA and server certificate for local TLS testing
# (Valkey/Redis TLS). Output: dev/certs/{ca.crt,server.crt,server.key}
set -eu
cd "$(dirname "$0")/.."
mkdir -p dev/certs
HOST_DIR="$(pwd -W 2>/dev/null || pwd)/dev/certs"
MSYS_NO_PATHCONV=1 docker run --rm -v "$HOST_DIR:/certs" alpine:3.22 sh -c '
  set -e
  apk add --no-cache openssl >/dev/null
  cd /certs
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=sf-archive-dev-ca" -keyout ca.key -out ca.crt 2>/dev/null
  printf "subjectAltName=DNS:localhost,DNS:valkey,DNS:redis,IP:127.0.0.1\n" > san.ext
  openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -extfile san.ext -out server.crt 2>/dev/null
  chmod 644 server.key ca.key
  rm -f server.csr san.ext ca.srl
'
echo "certificates written to dev/certs"
