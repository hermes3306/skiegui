from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://bstyfs23:eZVn53ELtH7jGF4G@cluster0.uo0pbrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Select a database (create it if it doesn't exist)
db = client["sample_database"]

# Create a collection (or use an existing one)
collection = db["sample_collection"]

# Insert some data
data_to_insert = [
    {"name": "John Doe", "age": 30, "city": "New York"},
    {"name": "Jane Smith", "age": 25, "city": "Los Angeles"},
    {"name": "Bob Johnson", "age": 35, "city": "Chicago"}
]
result = collection.insert_many(data_to_insert)
print(f"Inserted {len(result.inserted_ids)} documents")

# Select data from the collection
print("\nAll documents in the collection:")
for doc in collection.find():
    print(doc)

# Select data with a specific query
print("\nDocuments where age > 30:")
query = {"age": {"$gt": 30}}
for doc in collection.find(query):
    print(doc)

# Close the connection
client.close()