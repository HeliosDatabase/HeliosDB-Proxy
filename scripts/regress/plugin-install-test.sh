#!/usr/bin/env bash
# helios-plugin registry/install CLI test (Batch H, item 78).
#
# Drives the actual `helios-plugin` binary end-to-end against a local file://
# registry: scaffold, list, install (unsigned), install (Ed25519-signed +
# verified against a trust root), and the two rejection paths (sha256 mismatch,
# untrusted signer). Ed25519 keys/signatures are produced with openssl in the
# trust-root format the proxy loader expects (raw 32-byte pubkey / 64-byte sig).
set -u
BIN="${1:-./target/release/helios-plugin}"
OUT="${OUT:-/tmp/plugin-install-test}"; rm -rf "$OUT"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
b64(){ base64 -w0 "$1"; }

REG="$OUT/registry"; DEST="$OUT/plugins"; TRUST="$OUT/trust"
mkdir -p "$REG" "$DEST" "$TRUST"

# A pretend plugin artefact.
printf '\x00asm\x01\x00\x00\x00helios-demo-plugin' > "$REG/colmask.wasm"
SHA=$(sha256sum "$REG/colmask.wasm" | cut -d' ' -f1)

# Trusted publisher key (raw Ed25519 in the loader's trust-root format).
openssl genpkey -algorithm ed25519 -out "$OUT/official.pem" 2>/dev/null
openssl pkey -in "$OUT/official.pem" -pubout -outform DER 2>/dev/null | tail -c 32 | base64 -w0 > "$TRUST/official.pub"
openssl pkeyutl -sign -inkey "$OUT/official.pem" -rawin -in "$REG/colmask.wasm" -out "$OUT/colmask.sig" 2>/dev/null
SIG=$(b64 "$OUT/colmask.sig")

# An attacker key NOT in the trust root.
openssl genpkey -algorithm ed25519 -out "$OUT/evil.pem" 2>/dev/null
openssl pkeyutl -sign -inkey "$OUT/evil.pem" -rawin -in "$REG/colmask.wasm" -out "$OUT/evil.sig" 2>/dev/null
EVILSIG=$(b64 "$OUT/evil.sig")

# Signed registry index.
cat > "$REG/index.json" <<EOF
{ "schema_version": "1", "plugins": [
  { "name": "colmask", "version": "0.1.0", "description": "column masking",
    "artifact": "colmask.wasm", "sha256": "$SHA", "signature": "$SIG" } ] }
EOF

# 1. list
out=$("$BIN" list --registry "$REG/index.json" 2>&1)
echo "$out" | grep -q "colmask" && echo "$out" | grep -q "signed" && ok "list: shows signed colmask" || bad "list: $out"

# 2. new (scaffold)
out=$("$BIN" new my-plugin --dir "$OUT/scaffold" 2>&1)
[ -f "$OUT/scaffold/my-plugin/plugin.yaml" ] && [ -f "$OUT/scaffold/my-plugin/src/lib.rs" ] && ok "new: scaffolded skeleton" || bad "new: $out"

# 3. install with trust root -> signature verified, .wasm + .sig land
out=$("$BIN" install colmask --registry "$REG/index.json" --dest "$DEST" --trust-root "$TRUST" 2>&1)
if echo "$out" | grep -q "verified by 'official'" && [ -f "$DEST/colmask.wasm" ] && [ -f "$DEST/colmask.sig" ]; then
  ok "install: signed artefact verified + deployed"
else bad "install signed: $out"; fi
# deployed bytes match source
cmp -s "$DEST/colmask.wasm" "$REG/colmask.wasm" && ok "install: deployed bytes match source" || bad "install: bytes differ"

# 4. install without trust root -> succeeds but unverified
rm -rf "$DEST"/*; out=$("$BIN" install colmask --registry "$REG/index.json" --dest "$DEST" 2>&1)
echo "$out" | grep -q "not checked" && [ -f "$DEST/colmask.wasm" ] && ok "install: unsigned mode deploys (signature not checked)" || bad "install unsigned: $out"

# 5. sha256 mismatch -> rejected, nothing deployed
rm -rf "$DEST"/*
sed "s/$SHA/0000000000000000000000000000000000000000000000000000000000000000/" "$REG/index.json" > "$REG/bad-sha.json"
out=$("$BIN" install colmask --registry "$REG/bad-sha.json" --dest "$DEST" 2>&1)
if echo "$out" | grep -qi "sha256 mismatch" && [ ! -f "$DEST/colmask.wasm" ]; then ok "reject: sha256 mismatch blocks install"; else bad "sha256: $out"; fi

# 6. untrusted signer -> rejected
rm -rf "$DEST"/*
sed "s|\"signature\": \"$SIG\"|\"signature\": \"$EVILSIG\"|" "$REG/index.json" > "$REG/evil.json"
out=$("$BIN" install colmask --registry "$REG/evil.json" --dest "$DEST" --trust-root "$TRUST" 2>&1)
if echo "$out" | grep -qi "signature verification failed" && [ ! -f "$DEST/colmask.wasm" ]; then ok "reject: untrusted signer blocks install"; else bad "untrusted: $out"; fi

echo "== plugin-install test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
