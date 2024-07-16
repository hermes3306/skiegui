from neo4j import GraphDatabase

# Neo4j connection details
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4j_password"

neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def run_cypher_file(tx, file_path):
    with open(file_path, 'r') as file:
        cypher_script = file.read()
        statements = cypher_script.split(';')
        for statement in statements:
            if statement.strip():
                tx.run(statement)
                print(f"Executed: {statement.strip()}")

def main():
    with neo4j_driver.session() as session:
        session.execute_write(run_cypher_file, 'rel.cypher')
    
    neo4j_driver.close()

if __name__ == "__main__":
    main()