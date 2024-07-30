import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from neo4j import GraphDatabase
from pymongo import MongoClient
import csv
import configparser
from colorama import init, Fore, Style

init(autoreset=True)  # Initialize colorama

class PostgreSQLManager:
    def __init__(self, config):
        self.connection_params = {
            "dbname": config['database'],
            "user": config['user'],
            "password": config['password'],
            "host": config['host'],
            "port": config['port']
        }

    def connect(self):
        return psycopg2.connect(**self.connection_params)
    
    def test_connection(self):
        with self.connect() as conn:
            if not conn:
                return False
            try:
                with conn.cursor() as cur:
                    cur.execute('SELECT 1')
                return True
            except DatabaseError as e:
                print(f"PostgreSQL connection test failed: {e}")
                return False

    def execute_query(self, query, params=None, fetch=False):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()

    def delete_all_tables(self):
        conn = self.connect()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = cur.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    print(f"Dropping table: {table_name}")
                    cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                
                print("All tables have been dropped successfully.")
        
        except Exception as e:
            print(f"An error occurred: {e}")
        
        finally:
            conn.close()

    def create_table(self, table_name, columns):
        columns = [col.strip() for col in columns.split(',')]
        column_definitions = ','.join([f'"{col}" TEXT' for col in columns])
        create_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_definitions})'
        
        self.execute_query(create_query)
        print(f"Table '{table_name}' created in PostgreSQL.")

    def list_tables(self):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        tables = self.execute_query(query, fetch=True)
        return [table[0] for table in tables]

    def get_table_columns(self, table_name):
        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """
        return [row[0] for row in self.execute_query(query, (table_name,), fetch=True)]

    def insert_nodes(self, table_name, nodes, all_properties):
        if not nodes:
            print(f"No nodes to insert into table '{table_name}'.")
            return

        # Validate that all properties are strings (required by PostgreSQL)
        for node in nodes:
            if not all(isinstance(key, str) for key in node):
                raise ValueError("All node property names must be strings.")

        # Create a list of column names in the correct order
        columns = sorted(all_properties)

        # Prepare data for insertion (fill missing values with None)
        data = []
        for node in nodes:
            row = [node.get(col, None) for col in columns]
            data.append(tuple(row))

        # Use psycopg2's execute_values for bulk insert
        with self.connect() as conn:
            with conn.cursor() as cur:
                insert_query = f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES %s'
                execute_values(cur, insert_query, data)

        print(f"{len(nodes)} nodes inserted into table '{table_name}'.")

    def download_table_as_csv(self, table_name):
        columns = self.get_table_columns(table_name)
        query = f"SELECT * FROM {table_name}"
        rows = self.execute_query(query, fetch=True)

        with open(f'{table_name}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
        
        print(f"Table '{table_name}' downloaded as CSV.")

    def upload_csv_to_table(self, csv_file_name):
        table_name = csv_file_name.replace('.csv', '')
        with open(csv_file_name, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            columns = ', '.join(header)

        self.create_table(table_name, columns)

        with self.connect() as conn:
            with conn.cursor() as cur:
                with open(csv_file_name, 'r') as f:
                    cur.copy_expert(f'COPY "{table_name}" ({columns}) FROM STDIN CSV HEADER', f)
        print(f'Data from "{csv_file_name}" uploaded to PostgreSQL table "{table_name}".')

    def display_table_structure(self, table_name):
        query = """
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        columns = self.execute_query(query, (table_name,), fetch=True)
        
        if not columns:
            print(f"Table '{table_name}' not found or has no columns.")
            return

        print(f"\nStructure of table '{table_name}':")
        print("Column Name".ljust(30) + "Data Type".ljust(20) + "Max Length")
        print("-" * 70)
        for col in columns:
            col_name, data_type, max_length = col
            max_length = str(max_length) if max_length else 'N/A'
            print(f"{col_name.ljust(30)}{data_type.ljust(20)}{max_length}")

    def execute_custom_query(self, query):
        try:
            result = self.execute_query(query, fetch=True)
            if result:
                # Get column names
                with self.connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        col_names = [desc[0] for desc in cur.description]
                
                # Print results in a tabular format
                print("\nQuery Result:")
                print(" | ".join(col_names))
                print("-" * (sum(len(col) for col in col_names) + 3 * (len(col_names) - 1)))
                for row in result:
                    print(" | ".join(str(item) for item in row))
            else:
                print("Query executed successfully. No results to display.")
        except Exception as e:
            print(f"Error executing query: {e}")

class Neo4jManager:
    def __init__(self, config):
        self.driver = GraphDatabase.driver(config['url'], auth=(config['user'], config['password']))

    def close(self):
        self.driver.close()

    def execute_query(self, query, params=None):
        with self.driver.session() as session:
            result = session.run(query, params)
            return list(result)

    def delete_all_nodes(self):
        query = "MATCH (n) DETACH DELETE n"
        self.execute_query(query)
        print("All nodes and relationships have been deleted.")

    def create_node(self, label, properties):
        query = f"CREATE (n:{label} $props) RETURN n"
        result = self.execute_query(query, {"props": properties})
        return result[0]["n"]

    def get_node_labels(self):
        query = "CALL db.labels()"
        result = self.execute_query(query)
        return [record["label"] for record in result]

    def get_node_properties(self, label):
        query = f"MATCH (n:{label}) RETURN n"
        try:
            result = self.execute_query(query)
        except Exception as e:
            raise Exception(f"Error executing query: {e}")

        node_properties = {}

        for record in result:
            node = record["n"]
            node_dict = dict(node)

            for key, value in node_dict.items():
                if key not in node_properties:
                    node_properties[key] = []
                if value not in node_properties[key]:
                    node_properties[key].append(value)
        return node_properties

    def download_nodes_as_csv(self, label):
        query = f"MATCH (n:{label}) RETURN properties(n) AS props"
        result = self.execute_query(query)
        
        if not result:
            print(f"No nodes found with label '{label}'")
            return

        properties = self.get_node_properties(label)
        
        with open(f'{label}.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=properties)
            writer.writeheader()
            for record in result:
                writer.writerow(record["props"])
        
        print(f"Nodes with label '{label}' downloaded as CSV.")

    def upload_csv_to_nodes(self, csv_file_name):
        label = csv_file_name.replace('.csv', '').capitalize()
        
        with open(csv_file_name, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.create_node(label, row)

        print(f"Data from '{csv_file_name}' uploaded as nodes with label '{label}'.")

    def display_node_structure(self, label):
        properties = self.get_node_properties(label)
        
        if not properties:
            print(f"No nodes found with label '{label}' or the nodes have no properties.")
            return

        print(f"\nStructure of nodes with label '{label}':")
        print("Property Name".ljust(30) + "Sample Value")
        print("-" * 50)
        
        query = f"MATCH (n:{label}) RETURN properties(n) AS props LIMIT 1"
        result = self.execute_query(query)
        
        if result:
            sample_node = result[0]["props"]
            for prop in properties:
                sample_value = str(sample_node.get(prop, 'N/A'))[:20]  # Truncate long values
                print(f"{prop.ljust(30)}{sample_value}")

    def execute_custom_query(self, query):
        try:
            result = self.execute_query(query)
            if not result:
                print("Query executed successfully, but returned no results.")
                return

            keys = result[0].keys()

            header = " | ".join(str(key).ljust(15) for key in keys)
            print("\n" + header)
            print("-" * len(header))

            for record in result:
                row = " | ".join(str(record[key])[:15].ljust(15) for key in keys)
                print(row)

            print(f"\nTotal results: {len(result)}")

        except Exception as e:
            print(f"An error occurred while executing the query: {str(e)}")

class MongoDBManager:
    def __init__(self, config):
        if config['host'] == 'localhost' or config['host'].startswith('127.0.0.1'):
            mongodb_url = f"mongodb://{config['host']}:{config['port']}/{config['database']}"
        else:
            mongodb_url = f"mongodb+srv://{config['user']}:{urllib.parse.quote_plus(config['password'])}@{config['host']}/{config['database']}?retryWrites=true&w=majority"
        self.client = MongoClient(mongodb_url)
        self.db = self.client[config['database']]

    def close(self):
        self.client.close()

    def list_collections(self):
        return self.db.list_collection_names()

    def create_collection(self, collection_name):
        self.db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created.")

    def insert_document(self, collection_name, document):
        collection = self.db[collection_name]
        result = collection.insert_one(document)
        print(f"Document inserted with ID: {result.inserted_id}")

    def find_documents(self, collection_name, query={}):
        collection = self.db[collection_name]
        return list(collection.find(query))

    def update_document(self, collection_name, query, update):
        collection = self.db[collection_name]
        result = collection.update_one(query, {"$set": update})
        print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

    def delete_document(self, collection_name, query):
        collection = self.db[collection_name]
        result = collection.delete_one(query)
        print(f"Deleted: {result.deleted_count}")

    def drop_collection(self, collection_name):
        self.db.drop_collection(collection_name)
        print(f"Collection '{collection_name}' dropped.")

def read_config():
    config = configparser.ConfigParser()
    config.read('db.ini')
    return config

def display_menu():
    print("\n--- Database Manager ---")
    print("1. List all tables/labels/collections")
    print("2. Create a new table/node/collection")
    print("3. Upload CSV")
    print("4. Download as CSV")
    print("5. Delete all tables/nodes/collections")
    print("6. Display structure")
    print("7. Execute custom query")
    print("8. Exit")
    return input("Enter your choice (1-8): ")

def main():
    config = read_config()
    databases = config.sections()

    while True:
        print("\nAvailable databases:")
        for i, db in enumerate(databases, 1):
            print(f"{i}. {db}")
        choice = input("Choose a database (or 'q' to quit): ")
        
        if choice.lower() == 'q':
            break

        try:
            db_choice = databases[int(choice) - 1]
        except (ValueError, IndexError):
            print(Fore.RED + "Invalid choice. Please try again.")
            continue

        db_config = dict(config[db_choice])
        
        if db_choice == 'postgresql':
            manager = PostgreSQLManager(db_config)
        elif db_choice == 'neo4j':
            manager = Neo4jManager(db_config)
        elif db_choice == 'mongodb':
            manager = MongoDBManager(db_config)
        else:
            print(Fore.RED + f"Unsupported database type: {db_choice}")
            continue

        while True:
            choice = display_menu()

            try:
                if choice == '1':
                    if db_choice == 'postgresql':
                        tables = manager.list_tables()
                        print("\nCurrent tables:")
                        for table in tables:
                            print(table)
                    elif db_choice == 'neo4j':
                        labels = manager.get_node_labels()
                        print("\nCurrent node labels:")
                        for label in labels:
                            print(label)
                    elif db_choice == 'mongodb':
                        collections = manager.list_collections()
                        print("\nCurrent collections:")
                        for collection in collections:
                            print(collection)

                elif choice == '2':
                    if db_choice == 'postgresql':
                        table_name = input("Enter the new table name: ")
                        columns = input("Enter column names (comma-separated): ")
                        manager.create_table(table_name, columns)
                    elif db_choice == 'neo4j':
                        label = input("Enter the node label: ")
                        properties = {}
                        while True:
                            key = input("Enter property name (or press Enter to finish): ")
                            if not key:
                                break
                            value = input(f"Enter value for {key}: ")
                            properties[key] = value
                        manager.create_node(label, properties)
                    elif db_choice == 'mongodb':
                        collection_name = input("Enter the new collection name: ")
                        manager.create_collection(collection_name)

                elif choice == '3':
                    csv_file = input("Enter the CSV file name: ")
                    if db_choice == 'postgresql':
                        manager.upload_csv_to_table(csv_file)
                    elif db_choice == 'neo4j':
                        manager.upload_csv_to_nodes(csv_file)
                    elif db_choice == 'mongodb':
                        print("CSV upload not implemented for MongoDB")

                elif choice == '4':
                    if db_choice == 'postgresql':
                        table_name = input("Enter the table name to download: ")
                        manager.download_table_as_csv(table_name)
                    elif db_choice == 'neo4j':
                        label = input("Enter the node label to download: ")
                        manager.download_nodes_as_csv(label)
                    elif db_choice == 'mongodb':
                        print("CSV download not implemented for MongoDB")

                elif choice == '5':
                    confirm = input("Are you sure you want to delete all data? (yes/no): ")
                    if confirm.lower() == 'yes':
                        if db_choice == 'postgresql':
                            manager.delete_all_tables()
                        elif db_choice == 'neo4j':
                            manager.delete_all_nodes()
                        elif db_choice == 'mongodb':
                            for collection in manager.list_collections():
                                manager.drop_collection(collection)
                    else:
                        print("Operation cancelled.")

                elif choice == '6':
                    if db_choice == 'postgresql':
                        table_name = input("Enter the table name to display its structure: ")
                        manager.display_table_structure(table_name)
                    elif db_choice == 'neo4j':
                        label = input("Enter the node label to display its structure: ")
                        manager.display_node_structure(label)
                    elif db_choice == 'mongodb':
                        collection_name = input("Enter the collection name: ")
                        documents = manager.find_documents(collection_name, {})
                        if documents:
                            print(f"\nStructure of collection '{collection_name}':")
                            for key in documents[0].keys():
                                print(key)
                        else:
                            print(f"No documents found in collection '{collection_name}'")

                elif choice == '7':
                    query = input("Enter your query: ")
                    if db_choice == 'postgresql':
                        manager.execute_custom_query(query)
                    elif db_choice == 'neo4j':
                        manager.execute_custom_query(query)
                    elif db_choice == 'mongodb':
                        print("Custom queries not implemented for MongoDB")

                elif choice == '8':
                    break

                else:
                    print(Fore.RED + "Invalid choice. Please try again.")

            except Exception as e:
                print(Fore.RED + f"An error occurred: {str(e)}")

        if db_choice in ['neo4j', 'mongodb']:
            manager.close()

if __name__ == "__main__":
    main()