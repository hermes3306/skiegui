import psycopg2
from PostgreSQLManager import PostgreSQLManager
from Neo4jManager import Neo4jManager
import os

def main():
    # Initialize PostgreSQL manager
    db_manager = PostgreSQLManager(
        dbname="skie",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

    # Initialize Neo4j manager
    neo4j_manager = Neo4jManager(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="neo4j_password"
    )

    db_manager.delete_all_tables()

        # Fetch all labels from Neo4j
    label_list = neo4j_manager.fetch_labels()
    print(f"Labels found in Neo4j:{label_list}")

    # Iterate over each label in the list
    for label_name in label_list:
        neo4j_manager.download_nodes_as_csv(label_name)
        # Upload the CSV file to pgsql
        db_manager.upload_csv_to_table(f'{label_name}.csv')
        os.remove(f'{label_name}.csv')
        #print(f"Data from nodes with the lable ('{label_name})' migrated to pgsql successfully!")

 
if __name__ == "__main__":
    main()