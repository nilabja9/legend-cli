# Snowflake Authentication Troubleshooting Guide

This document summarizes the authentication issues encountered when connecting Legend Engine to Snowflake, along with the solutions implemented.

## Overview

Legend Engine supports multiple authentication strategies for Snowflake:
1. **SnowflakePublic** - Keypair authentication (recommended)
2. **MiddleTierUserNamePassword** - Username/password via credential provider

## Issues Encountered

### Issue 1: MiddleTierUserNamePassword Authentication Failure

**Error:**
```
java.util.NoSuchElementException: No value present
at MiddleTierUserNamePasswordAuthenticationStrategy.handleConnection
```

**Root Cause:**
The `MiddleTierUserNamePassword` auth strategy expects a credential provider that returns BOTH username AND password as a credential object. It does not work with simple key-value lookups from a properties file vault.

**Attempted Solutions:**
- Added credentials in various formats to `vault.properties`
- Tried different vault reference formats (`secret:key`, `secret.username`, etc.)

**Resolution:**
Switched to **SnowflakePublic** (keypair) authentication, which works reliably with the properties file vault.

---

### Issue 2: Base64 Decoding Error with Private Key

**Error:**
```
org.bouncycastle.util.encoders.DecoderException: unable to decode base64 string:
invalid characters encountered in base64 data
```

**Root Cause:**
The private key was stored in full PEM format (with `-----BEGIN PRIVATE KEY-----` headers and newlines), but Legend Engine expected raw base64-encoded content.

**Solution:**
Store the private key as a single-line base64 string without PEM headers:

```properties
# Wrong - PEM format with headers
SNOWFLAKE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----
MIIEvQ...
-----END PRIVATE KEY-----

# Correct - Raw base64 (no headers, single line)
SNOWFLAKE_PRIVATE_KEY=MIIEvQIBADANBgkqhkiG9w0BAQEFAASC...
```

---

### Issue 3: Encrypted Private Key Parsing Error

**Error:**
```
org.bouncycastle.openssl.PEMException: problem parsing ENCRYPTED PRIVATE KEY:
java.lang.IllegalArgumentException: unknown object in getInstance: org.bouncycastle.asn1.ASN1Integer
```

**Root Cause:**
Legend Engine's `EncryptedPrivateKeyFromVaultRule` expects an **encrypted** private key in PEM format, not an unencrypted PKCS#8 key. When provided with an unencrypted key, the BouncyCastle parser fails.

**Solution:**
Generate an encrypted private key with a passphrase:

```bash
# Generate encrypted private key
openssl pkcs8 -topk8 -inform PEM -outform PEM \
  -in ~/.snowflake/rsa_key.p8 \
  -out ~/.snowflake/rsa_key_encrypted.p8 \
  -v2 aes-256-cbc \
  -passout pass:YourPassphrase
```

Store in vault with escaped newlines:
```properties
SNOWFLAKE_PRIVATE_KEY=-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIFNTBf...\n-----END ENCRYPTED PRIVATE KEY-----
SNOWFLAKE_PASSPHRASE=YourPassphrase
```

---

## Working Configuration

### 1. Generate RSA Keypair

```bash
# Generate unencrypted private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.snowflake/rsa_key.p8 -nocrypt

# Extract public key
openssl rsa -in ~/.snowflake/rsa_key.p8 -pubout -out ~/.snowflake/rsa_key.pub

# Create encrypted version for Legend
openssl pkcs8 -topk8 -inform PEM -outform PEM \
  -in ~/.snowflake/rsa_key.p8 \
  -out ~/.snowflake/rsa_key_encrypted.p8 \
  -v2 aes-256-cbc \
  -passout pass:YourPassphrase
```

### 2. Assign Public Key to Snowflake User

```sql
-- Extract public key content (remove headers)
-- Then run in Snowflake:
ALTER USER YOUR_USERNAME SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
DESC USER YOUR_USERNAME;
```

### 3. Configure Legend Vault

Edit `/app/engine/config/vault.properties` in the Legend container:

```properties
# Snowflake keypair credentials (encrypted)
SNOWFLAKE_PRIVATE_KEY=-----BEGIN ENCRYPTED PRIVATE KEY-----\n<base64-content>\n-----END ENCRYPTED PRIVATE KEY-----
SNOWFLAKE_PASSPHRASE=YourPassphrase
```

**Important:** The PEM content must be on a single line with `\n` as literal escape sequences.

### 4. Legend Connection Configuration

The connection Pure code should use `SnowflakePublic` auth:

```
auth: SnowflakePublic
{
  publicUserName: 'YOUR_SNOWFLAKE_USER';
  privateKeyVaultReference: 'SNOWFLAKE_PRIVATE_KEY';
  passPhraseVaultReference: 'SNOWFLAKE_PASSPHRASE';
}
```

### 5. Using legend-cli

```bash
legend-cli model from-snowflake DATABASE_NAME \
  --schema SCHEMA_NAME \
  --auth-type keypair \
  --legend-user "SNOWFLAKE_USER" \
  --project-name "my-project"
```

---

## Verification Steps

### Test Keypair Directly with Snowflake

```python
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

with open("~/.snowflake/rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user="YOUR_USER",
    account="YOUR_ACCOUNT",
    private_key=pkb,
    warehouse="COMPUTE_WH",
    database="YOUR_DATABASE"
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_USER()")
print(cursor.fetchone())
```

### Check Legend Vault Configuration

```bash
docker exec legend-omnibus cat /app/engine/config/vault.properties
```

### Restart Legend Engine After Vault Changes

```bash
docker restart legend-omnibus
```

---

## Summary Table

| Issue | Error Keyword | Solution |
|-------|---------------|----------|
| Password auth fails | `NoSuchElementException` | Use keypair auth instead |
| Invalid base64 | `invalid characters encountered` | Remove PEM headers, single-line base64 |
| ASN1Integer error | `unknown object in getInstance` | Use encrypted key + passphrase |

---

## Related Files

- `~/.snowflake/rsa_key.p8` - Unencrypted private key (for direct Snowflake testing)
- `~/.snowflake/rsa_key_encrypted.p8` - Encrypted private key (for Legend vault)
- `~/.snowflake/rsa_key.pub` - Public key (assigned to Snowflake user)
- `/app/engine/config/vault.properties` - Legend Engine vault configuration
