"""Database-specific connection generation for Pure code."""

from abc import ABC, abstractmethod
from typing import Optional, List


class ConnectionGenerator(ABC):
    """Abstract base class for generating Pure connection definitions."""

    @abstractmethod
    def generate(self, database_name: str, store_path: str, **kwargs) -> str:
        """Generate Pure connection definition.

        Args:
            database_name: Name of the database
            store_path: Full path to the store definition
            **kwargs: Database-specific connection parameters

        Returns:
            Pure connection definition as a string
        """
        pass

    @property
    @abstractmethod
    def connection_type(self) -> str:
        """Return the Legend connection type (e.g., 'Snowflake', 'H2', 'DuckDB')."""
        pass


class SnowflakeConnectionGenerator(ConnectionGenerator):
    """Generates Snowflake connection definitions."""

    @property
    def connection_type(self) -> str:
        return "Snowflake"

    def generate(
        self,
        database_name: str,
        store_path: str,
        package_prefix: str = "model",
        account: str = "",
        warehouse: str = "",
        role: str = "ACCOUNTADMIN",
        region: Optional[str] = None,
        auth_type: str = "keypair",
        username: Optional[str] = None,
        private_key_vault_ref: str = "SNOWFLAKE_PRIVATE_KEY",
        passphrase_vault_ref: str = "SNOWFLAKE_PASSPHRASE",
        password_vault_ref: str = "SNOWFLAKE_PASSWORD",
        **kwargs
    ) -> str:
        """Generate Snowflake connection definition.

        Args:
            database_name: Name of the database
            store_path: Full path to the store definition
            package_prefix: Package prefix for Pure code
            account: Snowflake account identifier
            warehouse: Snowflake warehouse name
            role: Snowflake role (default: ACCOUNTADMIN)
            region: Snowflake region
            auth_type: Authentication type - 'keypair' or 'password'
            username: Snowflake username for the connection
            private_key_vault_ref: Vault reference for private key
            passphrase_vault_ref: Vault reference for passphrase
            password_vault_ref: Vault reference for password

        Returns:
            Pure connection definition as a string
        """
        lines = ["###Connection"]
        lines.append(f"RelationalDatabaseConnection {package_prefix}::connection::{database_name}Connection")
        lines.append("{")
        lines.append(f"  store: {store_path};")
        lines.append("  type: Snowflake;")
        lines.append("  specification: Snowflake")
        lines.append("  {")
        lines.append(f"    name: '{database_name}';")
        lines.append(f"    account: '{account}';")
        lines.append(f"    warehouse: '{warehouse}';")
        lines.append(f"    region: '{region or ''}';")
        lines.append(f"    role: '{role}';")
        lines.append("  };")

        if auth_type == "keypair":
            lines.append("  auth: SnowflakePublic")
            lines.append("  {")
            lines.append(f"    publicUserName: '{username or 'LEGEND_USER'}';")
            lines.append(f"    privateKeyVaultReference: '{private_key_vault_ref}';")
            lines.append(f"    passPhraseVaultReference: '{passphrase_vault_ref}';")
            lines.append("  };")
        else:
            lines.append("  auth: MiddleTierUserNamePassword")
            lines.append("  {")
            lines.append(f"    vaultReference: '{password_vault_ref}';")
            lines.append("  };")

        lines.append("}")
        return "\n".join(lines)


class DuckDBConnectionGenerator(ConnectionGenerator):
    """Generates DuckDB/LocalH2 connection definitions for Legend.

    Uses Legend's LocalH2 connection type which can be used for
    local file-based databases like DuckDB.
    """

    @property
    def connection_type(self) -> str:
        return "H2"

    def generate(
        self,
        database_name: str,
        store_path: str,
        package_prefix: str = "model",
        database_path: Optional[str] = None,
        test_data_sqls: Optional[List[str]] = None,
        **kwargs
    ) -> str:
        """Generate LocalH2 connection for DuckDB.

        Args:
            database_name: Name for the connection
            store_path: Full path to the store definition
            package_prefix: Package prefix for Pure code
            database_path: Path to DuckDB file (optional, for documentation)
            test_data_sqls: Optional SQL statements for test data setup

        Returns:
            Pure connection definition as a string
        """
        lines = ["###Connection"]
        lines.append(f"RelationalDatabaseConnection {package_prefix}::connection::{database_name}Connection")
        lines.append("{")
        lines.append(f"  store: {store_path};")
        lines.append("  type: H2;")
        lines.append("  specification: LocalH2")
        lines.append("  {")

        if test_data_sqls:
            # Format SQL statements for test data setup
            sql_lines = []
            for sql in test_data_sqls:
                escaped_sql = sql.replace("'", "\\'").replace("\n", "\\n")
                sql_lines.append(f"      '{escaped_sql}'")
            lines.append("    testDataSetupSqls: [")
            lines.append(",\n".join(sql_lines))
            lines.append("    ];")

        lines.append("  };")
        lines.append("  auth: DefaultH2;")
        lines.append("}")

        return "\n".join(lines)
