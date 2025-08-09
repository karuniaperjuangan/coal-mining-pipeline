from clickhouse_driver import Client
import pandas as pd


def create_clickhouse_table_from_mysql(
    ch_client,
    mysql_cursor,
    mysql_database,
    clickhouse_database,
    table_name,
    cluster_name,
):
    dtype_mapping = {
        "TINYINT": "Int8",
        "TINYINT UNSIGNED": "UInt8",
        "SMALLINT": "Int16",
        "SMALLINT UNSIGNED": "UInt16",
        "MEDIUMINT": "Int32",
        "MEDIUMINT UNSIGNED": "UInt32",
        "INT": "Int32",
        "INT UNSIGNED": "UInt32",
        "INTEGER": "Int32",
        "INTEGER UNSIGNED": "UInt32",
        "BIGINT": "Int64",
        "BIGINT UNSIGNED": "UInt64",
        "FLOAT": "Float32",
        "FLOAT UNSIGNED": "Float32",
        "DOUBLE": "Float64",
        "DOUBLE UNSIGNED": "Float64",
        "DECIMAL": "Float64",
        "DECIMAL UNSIGNED": "Float64",
        "NUMERIC": "Float64",
        "NUMERIC UNSIGNED": "Float64",
        "DATE": "Date",
        "DATETIME": "DateTime64",
        "TIMESTAMP": "DateTime64",
        "TIME": "String",
        "YEAR": "UInt16",
        "CHAR": "String",
        "VARCHAR": "String",
        "TINYTEXT": "String",
        "TEXT": "String",
        "MEDIUMTEXT": "String",
        "LONGTEXT": "String",
        "BINARY": "String",
        "VARBINARY": "String",
        "TINYBLOB": "String",
        "BLOB": "String",
        "MEDIUMBLOB": "String",
        "LONGBLOB": "String",
        "ENUM": "String",
        "SET": "String",
        "BOOLEAN": "UInt8",
        "BIT": "String",
    }
    # Check if the table exists in ClickHouse
    if cluster_name:
        ch_client.execute(
            f"CREATE DATABASE IF NOT EXISTS {clickhouse_database} ON CLUSTER {cluster_name}"
        )
    else:
        ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {clickhouse_database}")
    table_exists_result = ch_client.execute(
        f"EXISTS TABLE {clickhouse_database}.{table_name}"
    )  # Renamed variable

    if not table_exists_result[0][0]:  # Adjusted access to result
        print(f"Creating ClickHouse table {table_name}")
        # Fetch MySQL table schema
        mysql_cursor.execute(f"SHOW COLUMNS FROM {mysql_database}.{table_name}")
        columns_schema = mysql_cursor.fetchall()  # Renamed variable

        # Map MySQL types to ClickHouse types
        ch_columns = []
        print(columns_schema)
        for column_info in columns_schema:  # Renamed loop variable
            column_name = column_info["Field"]
            mysql_type = column_info["Type"].split("(")[0].upper()
            ch_type = dtype_mapping.get(
                mysql_type, "String"
            )  # Default to String if no mapping
            # Make column nullable if it doesn't have NOT NULL
            if column_info["Null"] == "YES":
                ch_type = f"Nullable({ch_type})"
            ch_columns.append(f"`{column_name}` {ch_type}")

        # Add additional columns
        ch_columns.extend(
            [
                "`ingested_at` DateTime DEFAULT formatDateTime(now(), '%Y-%m-%d %H:%i:%S', 'Asia/Jakarta')",
                "`version` UInt32 DEFAULT toUnixTimestamp(now())",
                # "`__deleted` Enum8('false' = 0, 'true' = 1) DEFAULT 0"
            ]
        )
        # Create ClickHouse table
        ch_columns_str = ",\n ".join(ch_columns)

        # Get the primary keys of MySQL table
        primary_key_columns = [
            col["Field"] for col in columns_schema if col["Key"] == "PRI"
        ]

        if not primary_key_columns:
            # try to find 'id' column first if any
            primary_key_columns = [
                col["Field"] for col in columns_schema if col["Field"].lower() == "id"
            ]
            # print(f"Primary key columns: {primary_key_columns}") # Optional: for debugging
            if not primary_key_columns:
                # if 'id' column is not found, use all columns that are not nullable
                primary_key_columns = [
                    col["Field"] for col in columns_schema if col["Null"] == "NO"
                ]

        if (
            not primary_key_columns
        ):  # Fallback if still no PKs (e.g. all columns are nullable and no 'id')
            print(
                f"Warning: No primary key found for {mysql_database}.{table_name}. Using `version` as part of ORDER BY."
            )
            primary_key_columns = [
                "version"
            ]  # Or handle as an error, or use a default like tuple()

        order_by_clause = (
            ", ".join(f"`{pk}`" for pk in primary_key_columns)
            if primary_key_columns
            else "tuple()"
        )

        if cluster_name:
            create_table_query = f"CREATE TABLE {clickhouse_database}.{table_name} ON CLUSTER {cluster_name} ({ch_columns_str}) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{{uuid}}/{{shard}}','{{replica}}', version) ORDER BY ({order_by_clause})"
        else:
            create_table_query = f"CREATE TABLE {clickhouse_database}.{table_name} ({ch_columns_str}) ENGINE = ReplacingMergeTree(version) ORDER BY ({order_by_clause})"  # Added version to ReplicatedReplacingMergeTree
        # print(f"Creating ClickHouse table:\n {create_table_query}")
        ch_client.execute(create_table_query)


def create_clickhouse_table_from_df(
    ch_client: Client,
    df: pd.DataFrame,
    database_name:str,
    table_name: str,
    cluster_name: str = None,
    primary_keys: list[str] = [],
):
    if cluster_name:
        ch_client.execute(
            f"CREATE DATABASE IF NOT EXISTS {database_name} ON CLUSTER {cluster_name}"
        )
    else:
        ch_client.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    dtype_mapping = {
        "int64": "Int64",
        "float64": "Float64",
        "bool": "UInt8",  # ClickHouse does not have a native Bool type
        "datetime64[ns]": "DateTime64(3)",
        "object": "String",  # Objects are typically strings
    }

    # Generate the column definitions for the CREATE TABLE statement
    column_definitions = []
    for col, dtype in df.dtypes.items():
        ch_type = dtype_mapping.get(
            str(dtype), "String"
        )  # Default to String for unknown types
        column_definitions.append(f"    `{col}` {ch_type}")

    column_definitions.extend(
        [
            "    `ingested_at` DateTime DEFAULT formatDateTime(now(), '%Y-%m-%d %H:%i:%S', 'Asia/Jakarta')",
            "    `version` UInt32 DEFAULT toUnixTimestamp(now())"
        ]
    )
    columns_sql = ",\n".join(column_definitions)

    # Determine the table engine based on the provided arguments
    engine_name = "MergeTree"
    if primary_keys:
        engine_name = "ReplacingMergeTree(version)"
        if cluster_name:
            engine_name = "ReplicatedReplacingMergeTree()"

    # Construct the full CREATE TABLE SQL statement
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    {f"ON CLUSTER {cluster_name}" if cluster_name else ""}
    (
    {columns_sql}
    )
    ENGINE = {engine_name}
    ORDER BY ({", ".join(f"`{pk}`" for pk in primary_keys)})
    """

    print("Executing SQL:\n", create_table_sql)
    ch_client.execute(create_table_sql)
    print(f"Table '{table_name}' created successfully.")
