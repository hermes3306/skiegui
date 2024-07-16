from neo4j import GraphDatabase
import csv

class Neo4jManager:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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
        # print(f"Node created with label '{label}' and properties: {properties}")
        return result[0]["n"]

    def get_node_labels(self):
        query = "CALL db.labels()"
        result = self.execute_query(query)
        return [record["label"] for record in result]

    def get_node_properties_by_1st_node(self, label):
        query = f"MATCH (n:{label}) RETURN properties(n) AS props LIMIT 1"
        result = self.execute_query(query)
        if result:
            return list(result[0]["props"].keys())
        return []
    
    def get_node_properties(self, label):
        query = f"MATCH (n:{label}) RETURN n"
        try:
            result = self.execute_query(query)
        except Exception as e:
            raise Exception(f"Error executing query: {e}")

        # Initialize empty dictionary to store properties and unique values
        node_properties = {}

        for record in result:
            node = record["n"]
            node_dict = dict(node)  # Convert node to dictionary

            # Update node_properties with unique values for each property
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

    def download_nodes_as_csv_v1(self, label):
        query = f"MATCH (n:{label}) RETURN properties(n) AS props"
        with self.driver.session() as session:
            results = session.run(query)
            # with open(f'{label_name}.csv', 'w', newline='', encoding='utf-8') as f:
            with open(f'{label}.csv', 'w', newline='') as f:
                writer = csv.writer(f)             
                # Write header (if there are results)
                if results.peek():
                    first_row = results.peek()["props"]
                    writer.writerow(first_row.keys())
                    # print(first_row.keys())
                # Write data rows
                for record in results:
                    writer.writerow(record["props"].values())
                    # print(record["properties"].values())
        print(f"Nodes with label '{label}' downloaded as CSV.")

    def fetch_labels(self):
        with self.driver.session() as session:
            result = session.run("CALL db.labels() YIELD label RETURN collect(label) AS labels")
            return result.single()['labels']

    def fetch_nodes(self, label_name):
        """Fetch all nodes with a specific label from Neo4j."""
        query = f"MATCH (n:{label_name}) RETURN properties(n) AS props"
        result = self.execute_query(query)
        return [record["props"] for record in result]

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

            # Get the keys (column names) from the first result
            keys = result[0].keys()

            # Print the column headers
            header = " | ".join(str(key).ljust(15) for key in keys)
            print("\n" + header)
            print("-" * len(header))

            # Print each row
            for record in result:
                row = " | ".join(str(record[key])[:15].ljust(15) for key in keys)
                print(row)

            print(f"\nTotal results: {len(result)}")

        except Exception as e:
            print(f"An error occurred while executing the query: {str(e)}")

def display_menu():
    print("\n--- Neo4j Database Manager ---")
    print("1. List all node labels")
    print("2. Create a new node")
    print("3. Upload CSV to create nodes")
    print("4. Download nodes as CSV")
    print("5. Delete all nodes")
    print("6. Display node structure")
    print("7. Execute custom Cypher query")
    print("8. Exit")
    return input("Enter your choice (1-8): ")

def main():
    neo4j_manager = Neo4jManager(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="neo4j_password"
    )


    try:
        while True:
            choice = display_menu()

            if choice == '1':
                labels = neo4j_manager.get_node_labels()
                print("\nCurrent node labels:")
                for label in labels:
                    print(label)

            elif choice == '2':
                label = input("Enter the node label: ")
                properties = {}
                while True:
                    key = input("Enter property name (or press Enter to finish): ")
                    if not key:
                        break
                    value = input(f"Enter value for {key}: ")
                    properties[key] = value
                neo4j_manager.create_node(label, properties)

            elif choice == '3':
                csv_file = input("Enter the CSV file name: ")
                neo4j_manager.upload_csv_to_nodes(csv_file)

            elif choice == '4':
                label = input("Enter the node label to download: ")
                neo4j_manager.download_nodes_as_csv(label)

            elif choice == '5':
                confirm = input("Are you sure you want to delete all nodes? (yes/no): ")
                if confirm.lower() == 'yes':
                    neo4j_manager.delete_all_nodes()
                else:
                    print("Operation cancelled.")

            elif choice == '6':
                label = input("Enter the node label to display its structure: ")
                neo4j_manager.display_node_structure(label)

            elif choice == '7':
                query = input("Enter your Cypher query: ")
                neo4j_manager.execute_custom_query(query)

            elif choice == '8':
                print("Exiting the program. Goodbye!")
                break

            else:
                print("Invalid choice. Please try again.")

    finally:
        neo4j_manager.close()

if __name__ == "__main__":
    main()