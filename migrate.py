from py2neo import Graph, Node, Relationship
import pyodbc
from decimal import Decimal

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


def load_data_to_neo4j(sql_connection, schema, graph):
    cursor = sql_connection.cursor()

    # Step 1: Fetch foreign key relationships for the schema
    fk_relationships = get_foreign_keys(cursor, schema)

    # Step 2: Get all tables in the schema
    cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema}'")
    tables = [table[0] for table in cursor.fetchall()]

    # Step 3: Iterate through each table and process its data
    for table in tables:
        cursor.execute(f"SELECT * FROM {schema}.{table}")
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
            node = graph.nodes.match(table, **unique_constraint).first()

            if not node:
                # If node doesn't exist, create a new one
                node = Node(table, **node_data)
                graph.merge(node, table, primary_key)  # Using MERGE to prevent duplication

            # Step 5: Dynamically handle foreign key relationships
            for fk in fk_relationships:
                # Access tuple data correctly (index-based)
                parent_table = fk[1]
                referenced_table = fk[2]
                parent_column = fk[3]
                referenced_column = fk[4]

                if parent_table == table:
                    # Find the corresponding referenced table
                    # Check if the row has a foreign key value
                    foreign_key_value = node_data.get(parent_column)
                    if foreign_key_value:
                        # Check if the referenced node exists in the referenced table
                        referenced_node = graph.nodes.match(referenced_table, **{referenced_column: foreign_key_value}).first()

                        if referenced_node:
                            # Create the relationship, ensuring no foreign key is in the node properties
                            relationship = Relationship(referenced_node, "RELATED_TO", node)
                            graph.merge(relationship)  # Merge to avoid duplicates

                        else:
                            # Create the referenced node if it doesn't exist yet
                            referenced_node_data = {referenced_column: foreign_key_value}
                            referenced_node = Node(referenced_table, **referenced_node_data)
                            graph.merge(referenced_node, referenced_table, referenced_column)

                            # Create the relationship after creating the referenced node
                            relationship = Relationship(referenced_node, "RELATED_TO", node)
                            graph.merge(relationship)  # Merge to avoid duplicates

    print("Data and relationships loaded into Neo4j successfully!")


# SQL Server connection details
sql_server_driver = 'ODBC Driver 17 for SQL Server'
#sql_server_host = r'DESKTOP-KCG5AP5\BUILD_DEMO_INS'  # Or '192.168.0.124'
sql_server_host = '192.168.150.95'  # Use this IP address
sql_server_port = 1433
sql_server_user = 'sa'
sql_server_password = 'sa@123'
sql_server_db = 'KnowledgeGraphDemo'
current_schema = "dbo"


# Connect to SQL Server
sql_connection = pyodbc.connect(
    f"DRIVER={{{sql_server_driver}}};"
    f"SERVER={sql_server_host},{sql_server_port};"
    f"DATABASE={sql_server_db};"
    f"UID={sql_server_user};"
    f"PWD={sql_server_password}"
)

# Connect to your Neo4j instance
neo4j_graph = Graph("bolt://neo4j:7687", auth=("neo4j", "ragu@25801"))

# Run the data migration
load_data_to_neo4j(sql_connection, current_schema, neo4j_graph)