from airflow import DAG
from airflow.operators.python import PythonOperator
from py2neo import Graph, Node, Relationship
import pyodbc
from decimal import Decimal
from datetime import datetime

# SQL Server connection details
sql_server_driver = 'ODBC Driver 17 for SQL Server'
sql_server_host = 'host.docker.internal'  # Use 'host.docker.internal' when using Docker for SQL Server
sql_server_port = 1433
sql_server_user = 'sa'
sql_server_password = 'sa@123'
sql_server_db = 'slol'
current_schema = "dbo"

# Neo4j connection details
neo4j_graph = Graph("bolt://neo4j:7687", auth=("neo4j", "cantier123"))

def get_foreign_keys(cursor, schema):
    """Fetches foreign key relationships between tables."""
    cursor.execute(f"""
        SELECT 
            fk.name AS FK_name,
            tp.name AS parent_table,
            ref.name AS referenced_table,
            c1.name AS parent_column,
            c2.name AS referenced_column
        FROM 
            sys.foreign_keys AS fk
            INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables AS tp ON fkc.parent_object_id = tp.object_id
            INNER JOIN sys.tables AS ref ON fkc.referenced_object_id = ref.object_id
            INNER JOIN sys.columns AS c1 ON fkc.parent_column_id = c1.column_id AND c1.object_id = tp.object_id
            INNER JOIN sys.columns AS c2 ON fkc.referenced_column_id = c2.column_id AND c2.object_id = ref.object_id
        WHERE tp.schema_id = SCHEMA_ID('{schema}')
    """)
    return cursor.fetchall()


def load_data_to_neo4j(**kwargs):
    """Load data from SQL Server to Neo4j."""
    sql_connection = pyodbc.connect(
        f"DRIVER={{{sql_server_driver}}};"
        f"SERVER={sql_server_host},{sql_server_port};"
        f"DATABASE={sql_server_db};"
        f"UID={sql_server_user};"
        f"PWD={sql_server_password}"
    )
    cursor = sql_connection.cursor()

    # Step 1: Fetch foreign key relationships for the schema
    fk_relationships = get_foreign_keys(cursor, current_schema)

    # Step 2: Get all tables in the schema
    cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{current_schema}'")
    tables = [table[0] for table in cursor.fetchall()]

    # Step 3: Iterate through each table and process its data
    for table in tables:
        cursor.execute(f"SELECT * FROM {current_schema}.{table}")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        # Step 4: Create nodes for each row in the table
        for row in rows:
            node_data = dict(zip(columns, row))

            # Convert Decimal to float where necessary
            for key, value in node_data.items():
                if isinstance(value, Decimal):
                    node_data[key] = float(value)

            # Use the first column (usually primary key) as the unique identifier
            primary_key = columns[0]  # Assuming the first column is the primary key

            # Ensure that the primary key exists in the node_data
            if primary_key not in node_data:
                print(f"Warning: Primary key '{primary_key}' not found in data for table '{table}'. Skipping this row.")
                continue  # Skip this row if primary key is missing

            # Merge the node based on the primary key or another unique attribute
            unique_constraint = {primary_key: node_data[primary_key]}

            # Match or create the node based on the primary key
            node = neo4j_graph.nodes.match(table, **unique_constraint).first()

            if not node:
                # If node doesn't exist, create a new one
                node = Node(table, **node_data)
                neo4j_graph.merge(node, table, primary_key)  # Using MERGE to prevent duplication

            # Step 5: Dynamically handle foreign key relationships
            for fk in fk_relationships:
                parent_table = fk[1]
                referenced_table = fk[2]
                parent_column = fk[3]
                referenced_column = fk[4]

                if parent_table == table:
                    # Find the corresponding referenced table
                    foreign_key_value = node_data.get(parent_column)
                    if foreign_key_value:
                        # Check if the referenced node exists in the referenced table
                        referenced_node = neo4j_graph.nodes.match(referenced_table, **{referenced_column: foreign_key_value}).first()

                        if referenced_node:
                            # Create the relationship
                            relationship = Relationship(referenced_node, "RELATED_TO", node)
                            neo4j_graph.merge(relationship)  # Merge to avoid duplicates

                        else:
                            # Create the referenced node if it doesn't exist
                            referenced_node_data = {referenced_column: foreign_key_value}
                            referenced_node = Node(referenced_table, **referenced_node_data)
                            neo4j_graph.merge(referenced_node, referenced_table, referenced_column)

                            # Create the relationship after creating the referenced node
                            relationship = Relationship(referenced_node, "RELATED_TO", node)
                            neo4j_graph.merge(relationship)  # Merge to avoid duplicates

    print("Data and relationships loaded into Neo4j successfully!")


# Define the Airflow DAG
with DAG(
    'sql_to_neo4j_migration',
    description='Migrate data from SQL Server to Neo4j',
    schedule_interval=None,  # Set the schedule or leave as None for manual triggering
    start_date=datetime(2025, 1, 17),
    catchup=False
) as dag:

    # Define the Airflow task
    load_data_task = PythonOperator(
        task_id='load_data_to_neo4j',
        python_callable=load_data_to_neo4j,
        provide_context=True,  # Ensure that Airflow context is passed to the function
    )

    load_data_task  # Only one task in this DAG
