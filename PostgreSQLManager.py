import psycopg2
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import csv

class PostgreSQLManager:
    def __init__(self, dbname, user, password, host, port):
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }

    def connect(self):
        return psycopg2.connect(**self.connection_params)
    
    def test_connection(self):
        with self.connect() as conn:
            if not self.connection:
                return False
            try:
                with self.connection.cursor() as cur:
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


    def get_table_names(self):
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """
        result = self.execute_query(query, fetch=True)
        return [row[0] for row in result]

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

        print(data)    

        # Use psycopg2's execute_values for bulk insert
        from psycopg2.extras import execute_values
        with self.connect() as conn:
            with conn.cursor() as cur:
                insert_query = f'INSERT INTO "{table_name}" ({", ".join(columns)}) VALUES %s'

                print(insert_query)

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

    def list_tables(self):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        tables = self.execute_query(query, fetch=True)
        return [table[0] for table in tables]

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

    def execute_ddl(self, ddl_statement):
        try:
            with self.connect() as conn:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    cur.execute(ddl_statement)
            print("DDL statement executed successfully.")
        except Exception as e:
            print(f"Error executing DDL statement: {e}")

def display_menu():
    print("\n--- PostgreSQL Database Manager ---")
    print("1. List all tables")
    print("2. Create a new table")
    print("3. Upload CSV to table")
    print("4. Download table as CSV")
    print("5. Delete all tables")
    print("6. Display table structure")
    print("7. Execute custom query")
    print("8. Execute DDL statement")
    print("9. Exit")
    return input("Enter your choice (1-9): ")

def main():
    db_manager = PostgreSQLManager(
        dbname="skie",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

    while True:
        choice = display_menu()

        if choice == '1':
            tables = db_manager.list_tables()
            print("\nCurrent tables:")
            for table in tables:
                print(table)

        elif choice == '2':
            table_name = input("Enter the new table name: ")
            columns = input("Enter column names (comma-separated): ")
            db_manager.create_table(table_name, columns)

        elif choice == '3':
            csv_file = input("Enter the CSV file name: ")
            db_manager.upload_csv_to_table(csv_file)

        elif choice == '4':
            table_name = input("Enter the table name to download: ")
            db_manager.download_table_as_csv(table_name)

        elif choice == '5':
            confirm = input("Are you sure you want to delete all tables? (yes/no): ")
            if confirm.lower() == 'yes':
                db_manager.delete_all_tables()
            else:
                print("Operation cancelled.")

        elif choice == '6':
            table_name = input("Enter the table name to display its structure: ")
            db_manager.display_table_structure(table_name)

        elif choice == '7':
            query = input("Enter your SQL query: ")
            db_manager.execute_custom_query(query)

        elif choice == '8':
            ddl_statement = input("Enter your DDL statement: ")
            db_manager.execute_ddl(ddl_statement)

        elif choice == '9':
            print("Exiting the program. Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
