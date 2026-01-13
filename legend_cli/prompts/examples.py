"""Example Pure code for few-shot learning."""

# Class examples from Snowflake model
CLASS_EXAMPLES = '''
Example 1 - Simple class with string fields:
```pure
Class model::domain::CompanyIndex
{
  companyId: String[0..1];
  companyName: String[0..1];
  entityLevel: String[0..1];
  ein: String[0..1];
  cik: String[0..1];
  permidCompanyId: String[0..1];
  primaryTicker: String[0..1];
  primaryExchangeCode: String[0..1];
  primaryExchangeName: String[0..1];
}
```

Example 2 - Class with mixed types (String, Date, Integer):
```pure
Class model::domain::SecReportAttribute
{
  variable: String[0..1];
  cik: String[0..1];
  adsh: String[0..1];
  measureDescription: String[0..1];
  tag: String[0..1];
  unitOfMeasure: String[0..1];
  value: String[0..1];
  statement: String[0..1];
  periodStartDate: Date[0..1];
  periodEndDate: Date[0..1];
  coveredQtrs: Integer[0..1];
}
```

Example 3 - Class with relationships:
```pure
Class model::domain::CompanyRelationship
{
  companyId: String[0..1];
  companyName: String[0..1];
  entityLevel: String[0..1];
  relatedCompanyId: String[0..1];
  relatedCompanyName: String[0..1];
  relatedEntityLevel: String[0..1];
  relationshipType: String[0..1];
  relationshipStartDate: Date[0..1];
  relationshipEndDate: Date[0..1];
}
```
'''

# Store examples
STORE_EXAMPLES = '''
Example - Snowflake Database with schema and tables:
```pure
###Relational
Database model::store::SnowflakeDB
(
  Schema CYBERSYN
  (
    Table COMPANY_INDEX
    (
      COMPANY_ID VARCHAR(256),
      COMPANY_NAME VARCHAR(1024),
      ENTITY_LEVEL VARCHAR(256),
      EIN VARCHAR(256),
      CIK VARCHAR(256),
      PERMID_COMPANY_ID VARCHAR(256),
      PRIMARY_TICKER VARCHAR(256),
      PRIMARY_EXCHANGE_CODE VARCHAR(256),
      PRIMARY_EXCHANGE_NAME VARCHAR(256)
    )
    Table SEC_REPORT_INDEX
    (
      CIK VARCHAR(256),
      COMPANY_NAME VARCHAR(1024),
      EIN VARCHAR(256),
      ADSH VARCHAR(256),
      FILED_DATE DATE,
      FORM_TYPE VARCHAR(256),
      FISCAL_PERIOD VARCHAR(256),
      FISCAL_YEAR VARCHAR(256)
    )
    Table SEC_REPORT_ATTRIBUTES
    (
      VARIABLE VARCHAR(256),
      CIK VARCHAR(256),
      ADSH VARCHAR(256),
      MEASURE_DESCRIPTION VARCHAR(1024),
      TAG VARCHAR(256),
      UNIT_OF_MEASURE VARCHAR(256),
      VALUE VARCHAR(1024),
      STATEMENT VARCHAR(256),
      PERIOD_START_DATE DATE,
      PERIOD_END_DATE DATE,
      COVERED_QTRS INTEGER
    )
  )
)
```
'''

# Connection examples
CONNECTION_EXAMPLES = '''
Example - Snowflake connection with public key authentication:
```pure
###Connection
RelationalDatabaseConnection model::connection::SnowflakeConnection
{
  store: model::store::SnowflakeDB;
  type: Snowflake;
  specification: Snowflake
  {
    name: 'SEC_FILINGS_DEMO_DATA';
    account: 'FJLCQXY-ZHB91196';
    warehouse: 'COMPUTE_WH';
    region: 'us-east-1';
    role: 'ACCOUNTADMIN';
  };
  auth: SnowflakePublic
  {
    publicUserName: 'DEV_NILABJA';
    privateKeyVaultReference: 'SNOWFLAKE_PRIVATE_KEY';
    passPhraseVaultReference: 'SNOWFLAKE_PASSPHRASE';
  };
}
```
'''

# Mapping examples
MAPPING_EXAMPLES = '''
Example - Relational mapping from class to table:
```pure
###Mapping
Mapping model::mapping::SnowflakeMapping
(
  model::domain::CompanyIndex: Relational
  {
    ~primaryKey
    (
      [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.COMPANY_ID
    )
    ~mainTable [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX
    companyId: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.COMPANY_ID,
    companyName: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.COMPANY_NAME,
    entityLevel: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.ENTITY_LEVEL,
    ein: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.EIN,
    cik: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.CIK,
    permidCompanyId: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.PERMID_COMPANY_ID,
    primaryTicker: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.PRIMARY_TICKER,
    primaryExchangeCode: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.PRIMARY_EXCHANGE_CODE,
    primaryExchangeName: [model::store::SnowflakeDB]CYBERSYN.COMPANY_INDEX.PRIMARY_EXCHANGE_NAME
  }
  model::domain::SecReportAttribute: Relational
  {
    ~primaryKey
    (
      [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.VARIABLE,
      [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.ADSH
    )
    ~mainTable [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES
    variable: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.VARIABLE,
    cik: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.CIK,
    adsh: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.ADSH,
    measureDescription: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.MEASURE_DESCRIPTION,
    tag: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.TAG,
    unitOfMeasure: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.UNIT_OF_MEASURE,
    value: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.VALUE,
    statement: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.STATEMENT,
    periodStartDate: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.PERIOD_START_DATE,
    periodEndDate: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.PERIOD_END_DATE,
    coveredQtrs: [model::store::SnowflakeDB]CYBERSYN.SEC_REPORT_ATTRIBUTES.COVERED_QTRS
  }
)
```
'''
