# Legend CLI

A powerful command-line tool for creating Legend artifacts using natural language prompts and automatic database schema introspection.

## Features

- **AI-Powered Code Generation**: Generate Pure code (classes, stores, connections, mappings) from natural language descriptions using Claude AI
- **Automatic Model Generation**: Introspect Snowflake databases and automatically generate complete Legend models
- **AI-Powered Documentation**: Automatically generate `doc.doc` descriptions for classes and attributes from external documentation sources (URLs, PDFs, JSON) or infer from naming conventions
- **Automatic Relationship Detection**: Analyze schema to detect foreign key relationships and generate associations between classes
- **Direct SDLC Integration**: Push generated code directly to Legend SDLC and commit changes
- **Project & Workspace Management**: Create and manage Legend projects and workspaces from the CLI

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/legend-cli.git
cd legend-cli

# Install with base dependencies
pip install -e .

# Install with Snowflake support (for automatic model generation)
pip install -e ".[snowflake]"
```

## Configuration

Set the following environment variables:

```bash
# Legend SDLC connection
export LEGEND_SDLC_URL="http://localhost:6900/sdlc/api"

# For AI-powered code generation
export ANTHROPIC_API_KEY="sk-ant-..."

# For Snowflake introspection
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
```

## Quick Start

### 1. Check Connection

```bash
# Verify Legend SDLC connection
legend-cli health

# Show current configuration
legend-cli config
```

### 2. Create a Project

```bash
legend-cli project create "My Data Model"
```

### 3. Generate Entities with AI

```bash
# Generate a class from natural language
legend-cli create class "A Customer with id, name, email, and registration date"

# Generate and push to SDLC
legend-cli create class "A Product with name, price, and category" --push --project 2
```

### 4. Generate Complete Model from Snowflake

```bash
# Automatically generate store, classes, connection, mapping, runtime
legend-cli model from-snowflake MY_DATABASE --schema MY_SCHEMA
```

## Commands

### Project Management

```bash
# List all projects
legend-cli project list

# Create a new project
legend-cli project create "Project Name" --description "Description"

# Get project details
legend-cli project info <project-id>
```

### Workspace Management

```bash
# List workspaces
legend-cli workspace list <project-id>

# Create a workspace
legend-cli workspace create <project-id> my-workspace

# List entities in a workspace
legend-cli workspace entities <project-id> <workspace-id>
```

### AI-Powered Entity Creation

Generate Pure code from natural language descriptions:

```bash
# Generate a class
legend-cli create class "A Person with firstName, lastName, email, and birthDate"

# Generate a database store
legend-cli create store "Snowflake database called ProductDB with schema SALES containing PRODUCTS table with columns id, name, price, category"

# Generate a connection
legend-cli create connection "Snowflake connection to account ABC123, warehouse COMPUTE_WH, database ProductDB"

# Generate a mapping
legend-cli create mapping "Map Product class to PRODUCTS table in ProductDB"
```

#### Options for create commands

| Option | Description |
|--------|-------------|
| `--push` | Push generated code to Legend SDLC |
| `--project` | Project ID to push to |
| `--workspace` | Workspace ID (default: dev-workspace) |
| `--message` | Commit message |
| `--output` | Save generated code to file |
| `--package` | Package path (e.g., model::domain) |

#### Examples with options

```bash
# Generate and push a class
legend-cli create class "An Order with orderId, items, total" \
  --push --project 2 --message "Added Order class"

# Generate and save to file
legend-cli create store "PostgreSQL database with users table" \
  --output ./my_store.pure

# Push existing Pure file
legend-cli create from-file ./model.pure --project 2
```

### Automatic Model Generation from Snowflake

The most powerful feature - automatically generate a complete Legend model from a Snowflake database:

```bash
legend-cli model from-snowflake <DATABASE_NAME> [OPTIONS]
```

#### Options

| Option | Description |
|--------|-------------|
| `--schema, -s` | Specific schema to introspect (default: all) |
| `--project-name, -n` | Name for new Legend project |
| `--project-id, -p` | Use existing project ID |
| `--workspace, -w` | Workspace ID (default: dev-workspace) |
| `--account, -a` | Snowflake account |
| `--user, -u` | Snowflake user |
| `--password` | Snowflake password |
| `--warehouse` | Snowflake warehouse |
| `--role, -r` | Snowflake role (default: ACCOUNTADMIN) |
| `--region` | Snowflake region (default: us-east-1) |
| `--dry-run` | Preview without pushing |
| `--output, -o` | Save Pure files to directory |

**Documentation Generation Options:**

| Option | Description |
|--------|-------------|
| `--doc-source, -d` | Documentation source (URL, PDF, or JSON). Can be specified multiple times |
| `--auto-docs` | Auto-generate documentation from class/attribute names using AI |

**Authentication Options (for Legend Connection):**

| Option | Description |
|--------|-------------|
| `--auth-type` | Auth type: `keypair` (default) or `password` |
| `--legend-user` | Snowflake username for Legend connection (defaults to --user) |
| `--private-key-ref` | Vault reference for private key (default: SNOWFLAKE_PRIVATE_KEY) |
| `--passphrase-ref` | Vault reference for passphrase (default: SNOWFLAKE_PASSPHRASE) |
| `--password-ref` | Vault reference for password (default: SNOWFLAKE_PASSWORD) |
| `--aws-secret` | AWS Secrets Manager secret name (e.g., `legend/snowflake/credentials`) |

#### Snowflake Authentication in Legend

Legend uses **vault references** for credentials - these are named placeholders that Legend resolves at runtime from a configured credential store (AWS Secrets Manager, HashiCorp Vault, etc.).

**Key-pair Authentication (recommended for production):**
```bash
legend-cli model from-snowflake MY_DB \
  --legend-user "SERVICE_ACCOUNT" \
  --private-key-ref "PROD_SNOWFLAKE_KEY" \
  --passphrase-ref "PROD_SNOWFLAKE_PASSPHRASE"
```

Generated connection:
```pure
auth: SnowflakePublic
{
  publicUserName: 'SERVICE_ACCOUNT';
  privateKeyVaultReference: 'PROD_SNOWFLAKE_KEY';
  passPhraseVaultReference: 'PROD_SNOWFLAKE_PASSPHRASE';
};
```

**Password Authentication (MiddleTier):**
```bash
legend-cli model from-snowflake MY_DB \
  --auth-type password \
  --legend-user "MY_USER" \
  --password-ref "MY_PASSWORD_SECRET"
```

Generated connection:
```pure
auth: MiddleTierUserNamePassword
{
  vaultReference: 'MY_PASSWORD_SECRET';
};
```

**AWS Secrets Manager Authentication (Recommended):**
```bash
legend-cli model from-snowflake MY_DB \
  --auth-type password \
  --legend-user "MY_USER" \
  --aws-secret "legend/snowflake/credentials"
```

Generated connection:
```pure
auth: MiddleTierUserNamePassword
{
  vaultReference: 'legend/snowflake/credentials:password';
};
```

#### Examples

```bash
# Generate model for entire database
legend-cli model from-snowflake FACTSET_DATA

# Generate model for specific schema
legend-cli model from-snowflake SEC_FILINGS --schema CYBERSYN

# Preview without pushing (dry run)
legend-cli model from-snowflake MY_DB --schema ANALYTICS --dry-run

# Save generated files locally
legend-cli model from-snowflake MY_DB --output ./generated_pure

# Use existing project
legend-cli model from-snowflake MY_DB --project-id 5

# Full example with custom authentication
legend-cli model from-snowflake PROD_DATABASE \
  --schema ANALYTICS \
  --project-name "analytics-model" \
  --account ABC123-XYZ \
  --warehouse COMPUTE_WH \
  --role ANALYST \
  --legend-user "LEGEND_SERVICE_ACCOUNT" \
  --private-key-ref "PROD_SF_PRIVATE_KEY" \
  --passphrase-ref "PROD_SF_PASSPHRASE"
```

#### What gets generated

| Artifact | Description |
|----------|-------------|
| **Store** | Database definition with all tables/views, columns, and Join definitions |
| **Classes** | One class per table with properties matching columns |
| **Associations** | Separate Association elements defining relationships between classes |
| **Connection** | Snowflake connection with authentication config |
| **Mapping** | Relational mapping from classes to tables + AssociationMappings |
| **Runtime** | Runtime configuration linking mapping and connection |

### Automatic Relationship Detection

The CLI automatically analyzes your database schema to detect relationships between tables:

**How it works:**
- Detects foreign key patterns like `COMPANY_ID` → `COMPANY_INDEX` table
- Recognizes common suffixes: `_ID`, `_KEY`, `_CODE`
- Identifies index/reference tables: `*_INDEX`, `*_MASTER`, `*_DIM`, `*_LOOKUP`, `*_REF`
- Creates proper `Association` elements (not just properties in classes)
- Generates Join definitions in the store
- Adds `AssociationMapping` entries that reference the joins

**Example output:**
```
Detected Relationships:
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Source                      ┃ Target                     ┃ Type        ┃ Property       ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ COMPANY_CHARACTERISTICS.CIK │ SEC_CIK_INDEX.CIK          │ many_to_one │ secCik         │
│ COMPANY_INDEX.COMPANY_ID    │ COMPANY_INDEX.COMPANY_ID   │ many_to_one │ company        │
│ SEC_REPORT_INDEX.CIK        │ SEC_CIK_INDEX.CIK          │ many_to_one │ secCik         │
└─────────────────────────────┴────────────────────────────┴─────────────┴────────────────┘
```

**Generated Pure code includes:**

```pure
// Class with regular properties only
Class model::domain::SecReportIndex
{
  cik: String[0..1];
  adsh: String[0..1];
  companyName: String[0..1];
  // ... other column properties
}

// Association element (separate from class)
Association model::domain::SecReportIndex_SecCikIndex_secCik
{
  secReportIndexs: model::domain::SecReportIndex[*];
  secCik: model::domain::SecCikIndex[0..1];
}

// Store with Join definition
Database model::store::MyDatabase
(
  Schema CYBERSYN
  (
    Table SEC_REPORT_INDEX (...)
    Table SEC_CIK_INDEX (...)
  )
  Join SEC_REPORT_INDEX_SEC_CIK_INDEX(CYBERSYN.SEC_REPORT_INDEX.CIK = CYBERSYN.SEC_CIK_INDEX.CIK)
)

// Mapping with class mappings and association mappings
Mapping model::mapping::MyDatabaseMapping
(
  // Class mapping
  model::domain::SecReportIndex: Relational
  {
    ~mainTable [model::store::MyDatabase]CYBERSYN.SEC_REPORT_INDEX
    cik: [model::store::MyDatabase]CYBERSYN.SEC_REPORT_INDEX.CIK
    // ... other property mappings
  }

  // Association mapping
  model::domain::SecReportIndex_SecCikIndex_secCik: Relational
  {
    AssociationMapping
    (
      secCik: [model::store::MyDatabase]@SEC_REPORT_INDEX_SEC_CIK_INDEX
    )
  }
)
```

### AI-Powered Documentation Generation

Legend CLI can automatically generate documentation (`doc.doc` tagged values) for your classes and attributes using Claude AI. This documentation appears in Legend Studio and helps users understand your data model.

#### How It Works

1. **With Documentation Sources** (`--doc-source`): Parse external documentation (websites, PDFs, JSON files) and intelligently match content to your classes and attributes
2. **Auto-Generation** (`--auto-docs`): Infer documentation from class/attribute names using common patterns (e.g., `user_id` → "Unique identifier for the user")
3. **Hybrid Approach**: When using `--doc-source`, unmatched items automatically fall back to name-based inference

#### Examples

```bash
# Auto-generate documentation from class/attribute names
legend-cli model from-snowflake SEC_FILINGS_DEMO_DATA \
  --schema CYBERSYN \
  --auto-docs \
  --dry-run

# Use external documentation sources
legend-cli model from-snowflake SEC_FILINGS_DEMO_DATA \
  --schema CYBERSYN \
  --doc-source https://docs.cybersyn.com/data-dictionary \
  --doc-source /path/to/schema-docs.pdf \
  --dry-run

# Combine with JSON data dictionary
legend-cli model from-snowflake MY_DATABASE \
  --doc-source /path/to/data-dictionary.json \
  --project-name "documented-model"
```

#### Supported Documentation Sources

| Source Type | Example | Description |
|-------------|---------|-------------|
| **URL** | `https://docs.example.com/schema` | Web pages with table/column descriptions |
| **PDF** | `/path/to/data-dictionary.pdf` | PDF documents with schema documentation |
| **JSON** | `/path/to/metadata.json` | Structured data dictionaries |

#### Generated Pure Code

With documentation enabled, classes and properties include `doc.doc` tagged values:

```pure
Class {meta::pure::profiles::doc.doc = 'SEC filing report index containing metadata about submitted filings.'} model::domain::SecReportIndex
{
  {meta::pure::profiles::doc.doc = 'Central Index Key (SEC company identifier).'} cik: String[0..1];
  {meta::pure::profiles::doc.doc = 'Accession Number (SEC filing identifier).'} adsh: String[0..1];
  {meta::pure::profiles::doc.doc = 'Name of the reporting company.'} companyName: String[0..1];
  {meta::pure::profiles::doc.doc = 'Date when the report was filed.'} reportDate: Date[0..1];
  {meta::pure::profiles::doc.doc = 'Fiscal year for financial reporting.'} fiscalYear: Integer[0..1];
}
```

#### Name-Based Inference Patterns

When using `--auto-docs` or when documentation sources don't match, the CLI infers documentation from common naming patterns:

| Pattern | Example | Generated Documentation |
|---------|---------|------------------------|
| `*_id` | `user_id` | "Unique identifier for the user." |
| `*_date`, `*_at` | `created_at` | "Date when created occurred." |
| `*_name` | `company_name` | "Name of the company." |
| `*_count`, `*_num` | `order_count` | "Number of orders." |
| `*_amount`, `*_value` | `total_amount` | "Value of total." |
| `is_*`, `*_flag` | `is_active` | "Indicates whether active." |
| `cik` | `cik` | "Central Index Key (SEC company identifier)." |
| `adsh` | `adsh` | "Accession Number (SEC filing identifier)." |
| `ein` | `ein` | "Employer Identification Number." |

### Snowflake Utilities

```bash
# List available databases
legend-cli model list-databases

# List schemas in a database
legend-cli model list-schemas MY_DATABASE
```

## Architecture

```
legend-cli/
├── legend_cli/
│   ├── main.py              # CLI entry point
│   ├── config.py            # Configuration management
│   ├── sdlc_client.py       # Legend SDLC API client
│   ├── engine_client.py     # Legend Engine API client
│   ├── claude_client.py     # Claude AI integration
│   ├── snowflake_client.py  # Snowflake introspection
│   ├── doc_generator.py     # AI documentation generation
│   ├── parsers/             # Document parsers
│   │   ├── base.py          # Base parser classes
│   │   ├── url_parser.py    # Web page parser
│   │   ├── pdf_parser.py    # PDF document parser
│   │   └── json_parser.py   # JSON data dictionary parser
│   ├── prompts/
│   │   ├── templates.py     # AI prompt templates
│   │   ├── examples.py      # Few-shot examples
│   │   └── doc_templates.py # Documentation generation prompts
│   └── commands/
│       ├── project.py       # Project commands
│       ├── workspace.py     # Workspace commands
│       ├── create.py        # Entity creation commands
│       └── model.py         # Model generation commands
```

## Requirements

- Python 3.9+
- Legend Omnibus or Legend SDLC running locally
- Anthropic API key (for AI features)
- Snowflake account (for model generation)

## Dependencies

- **typer**: CLI framework
- **httpx**: HTTP client
- **anthropic**: Claude AI SDK
- **pydantic**: Data validation
- **rich**: Terminal formatting
- **beautifulsoup4**: HTML parsing for URL documentation sources
- **lxml**: Fast XML/HTML parser
- **pypdf**: PDF document parsing
- **snowflake-connector-python**: Snowflake connectivity (optional)

## Examples

### Complete Workflow Example

```bash
# 1. Set up environment
export ANTHROPIC_API_KEY="sk-ant-..."
export SNOWFLAKE_ACCOUNT="ABC123"
export SNOWFLAKE_USER="analyst"
export SNOWFLAKE_PASSWORD="..."
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"

# 2. Generate complete model from Snowflake
legend-cli model from-snowflake PROD_DATA \
  --schema SALES \
  --project-name "sales-analytics"

# 3. Add additional entities with AI
legend-cli create class "A SalesReport with region, quarter, revenue, and growth percentage" \
  --push --project 4

# 4. View in Legend Studio
# Open http://localhost:6900/studio/edit/4/dev-workspace
```

### Using with Existing Project

```bash
# List projects to find ID
legend-cli project list

# Add entities to existing project
legend-cli create class "A new entity" --push --project 2 --workspace main

# Check what was created
legend-cli workspace entities 2 main
```

## AWS Secrets Manager Setup

For production use, AWS Secrets Manager provides secure credential storage that Legend Engine can access at runtime.

### 1. Create Secret in AWS

```bash
# Install AWS CLI if not already installed
brew install awscli

# Configure AWS credentials
aws configure

# Create the secret with Snowflake credentials
aws secretsmanager create-secret \
  --name "legend/snowflake/credentials" \
  --description "Snowflake credentials for Legend Platform" \
  --secret-string '{
    "username": "YOUR_SNOWFLAKE_USER",
    "password": "YOUR_SNOWFLAKE_PASSWORD",
    "account": "YOUR_ACCOUNT_ID",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN"
  }' \
  --region us-east-1
```

### 2. Configure Legend Engine

Legend Engine needs to be configured with AWS Secrets Manager vault and have access to AWS credentials.

**Option A: Use the startup script (recommended)**

Create a startup script `start-legend-omnibus.sh`:

```bash
#!/bin/bash
# Start Legend Omnibus with AWS Secrets Manager support

# Extract AWS credentials
AWS_ACCESS_KEY_ID=$(grep aws_access_key_id ~/.aws/credentials | head -1 | cut -d= -f2 | tr -d ' ')
AWS_SECRET_ACCESS_KEY=$(grep aws_secret_access_key ~/.aws/credentials | head -1 | cut -d= -f2 | tr -d ' ')

# Stop existing container
docker stop legend-omnibus 2>/dev/null
docker rm legend-omnibus 2>/dev/null

# Start with AWS credentials
docker run -d \
    --name legend-omnibus \
    -p 6900:6900 \
    -e LEGEND_OMNIBUS_CONFIG_GITLAB_PAT="your-gitlab-pat" \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_REGION="us-east-1" \
    finos/legend-omnibus:latest-slim

# Wait for startup
sleep 10

# Add AWS Secrets Manager vault to engine config
docker exec legend-omnibus sed -i '/^vaults:/a\  - _type: awsSecretsManager\n    region: us-east-1' \
    /app/engine/config/engine-config.yaml

echo "Legend Omnibus started with AWS Secrets Manager support"
echo "Studio: http://localhost:6900/studio"
```

**Option B: Manual configuration**

Add AWS Secrets Manager to Legend Engine's `engine-config.yaml`:

```yaml
vaults:
  - _type: awsSecretsManager
    region: us-east-1
  - _type: property
    location: /config/vault.properties
```

### 3. Generate Model with AWS Secret

```bash
legend-cli model from-snowflake MY_DATABASE \
  --schema MY_SCHEMA \
  --auth-type password \
  --legend-user "MY_USER" \
  --aws-secret "legend/snowflake/credentials" \
  --project-name "my-project"
```

### AWS Secret Format

When using `--aws-secret`, the vault references are automatically formatted as `secretName:jsonKey`:

| Auth Type | Vault Reference Format |
|-----------|----------------------|
| `password` | `legend/snowflake/credentials:password` |
| `keypair` | `legend/snowflake/credentials:private_key` |
| `keypair` | `legend/snowflake/credentials:passphrase` |

### Verifying AWS Secret

```bash
# List secrets
aws secretsmanager list-secrets --region us-east-1

# Get secret value
aws secretsmanager get-secret-value \
  --secret-id "legend/snowflake/credentials" \
  --region us-east-1
```

## Troubleshooting

### Connection Issues

```bash
# Check SDLC health
legend-cli health

# Verify configuration
legend-cli config
```

### Snowflake Issues

```bash
# Test connection with list-databases
legend-cli model list-databases

# Check available schemas
legend-cli model list-schemas YOUR_DATABASE
```

### Common Errors

| Error | Solution |
|-------|----------|
| "Cannot connect to Legend SDLC" | Ensure Legend Omnibus is running |
| "Anthropic API key not configured" | Set ANTHROPIC_API_KEY environment variable |
| "snowflake-connector-python required" | Install with `pip install -e ".[snowflake]"` |
| "No tables found" | Check schema name; views are included automatically |
| "Unable to locate credentials" | Run `aws configure` to set up AWS credentials |
| "Access denied to secret" | Check IAM permissions for Secrets Manager access |
| "Secret not found" | Verify secret name and region match |

### AWS Secrets Manager Issues

```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check if secret exists
aws secretsmanager describe-secret --secret-id "legend/snowflake/credentials"

# Verify Legend Engine has AWS vault configured
docker exec legend-omnibus grep -A 5 "vaults:" /app/engine/config/engine-config.yaml
```

## License

Apache 2.0

## Contributing

Contributions welcome! Please open an issue or pull request.
