import configparser
from neo4j import GraphDatabase

def read_config(file_path='db.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def get_neo4j_config(config):
    return {
        'uri': config['neo4j']['url'],
        'user': config['neo4j']['user'],
        'password': config['neo4j']['password']
    }

def run_cypher_file(tx, file_path):
    with open(file_path, 'r') as file:
        cypher_script = file.read()
        statements = cypher_script.split(';')
        for statement in statements:
            if statement.strip():
                tx.run(statement)
                print(f"Executed: {statement.strip()}")

def main():
    config = read_config()
    neo4j_config = get_neo4j_config(config)

    neo4j_driver = GraphDatabase.driver(neo4j_config['uri'], auth=(neo4j_config['user'], neo4j_config['password']))

    try:
        with neo4j_driver.session() as session:
            session.execute_write(run_cypher_file, 'init.cypher')
        
        print("All Cypher statements executed successfully.")
    finally:
        neo4j_driver.close()

if __name__ == "__main__":
    main()