# TLS test fixtures (test-only, NOT secrets)

Self-signed material used by the unit tests in `src/client_tls.rs`. The keys protect
nothing; they exist so `build_tls_acceptor` can be tested against every PEM key encoding
the proxy accepts. Regenerate with:

```sh
openssl ecparam -name prime256v1 -genkey -noout -out ca.key.sec1
openssl req -x509 -new -key ca.key.sec1 -sha256 -days 36500 -subj "/CN=heliosproxy-test-ca" -out ca.pem
openssl ecparam -name prime256v1 -genkey -noout -out server-ec.sec1.pem        # SEC1 "EC PRIVATE KEY"
openssl pkcs8 -topk8 -nocrypt -in server-ec.sec1.pem -out server-ec.pkcs8.pem  # PKCS#8 "PRIVATE KEY"
openssl req -new -key server-ec.sec1.pem -subj "/CN=localhost" -out server-ec.csr
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.ext
openssl x509 -req -in server-ec.csr -CA ca.pem -CAkey ca.key.sec1 -CAcreateserial -days 36500 -sha256 -extfile san.ext -out server-ec.pem
openssl genrsa -traditional -out server-rsa.pkcs1.pem 2048                      # PKCS#1 "RSA PRIVATE KEY"
openssl req -x509 -new -key server-rsa.pkcs1.pem -sha256 -days 36500 -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" -out server-rsa.pem
rm -f server-ec.csr san.ext ca.srl ca.key.sec1
```
