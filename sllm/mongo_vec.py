import pymongo
from pymongo import MongoClient
import numpy as np
from bson.binary import Binary
import pickle

# uri = "mongodb+srv://bstyfs23:eZVn53ELtH7jGF4G@cluster0.uo0pbrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# client = MongoClient(uri, server_api=ServerApi('1'))

# Connect to MongoDB
client = MongoClient('mongodb+srv://bstyfs23:eZVn53ELtH7jGF4G@cluster0.uo0pbrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['vector_search_db']
collection = db['documents']

def generate_random_vector(dim=100):
    return np.random.rand(dim).astype(np.float32)

def insert_sample_documents(num_docs=1000):
    documents = []
    for i in range(num_docs):
        doc = {
            'title': f'Document {i}',
            'content': f'This is the content of document {i}',
            'embedding': Binary(pickle.dumps(generate_random_vector(), protocol=2), subtype=128)
        }
        documents.append(doc)
    
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents")

# Create the index for vector search
def create_vector_index():
    collection.create_index([("embedding", "vectorSearch")], 
                            vectorSearchOptions={
                                "kind": "cosine",
                                "numDimensions": 100
                            })
    print("Vector index created")

# Insert sample documents and create index
insert_sample_documents()
create_vector_index()

def vector_similarity_search(query_vector, num_results=5):
    pipeline = [
        {
            "$vectorSearch": {
                "index": "embedding",
                "path": "embedding",
                "queryVector": Binary(pickle.dumps(query_vector, protocol=2), subtype=128),
                "numCandidates": 100,
                "limit": num_results
            }
        },
        {
            "$project": {
                "title": 1,
                "content": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results

def vector_similarity_search(query_vector, num_results=5):
    pipeline = [
        {
            "$vectorSearch": {
                "index": "embedding",
                "path": "embedding",
                "queryVector": Binary(pickle.dumps(query_vector, protocol=2), subtype=128),
                "numCandidates": 100,
                "limit": num_results
            }
        },
        {
            "$project": {
                "title": 1,
                "content": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results

def test_vector_search():
    # Generate a random query vector
    query_vector = generate_random_vector()
    
    # Perform vector similarity search
    results = vector_similarity_search(query_vector)
    
    print("Vector Search Results:")
    for i, result in enumerate(results, 1):
        print(f"{i}. Title: {result['title']}")
        print(f"   Score: {result['score']}")
        print(f"   Content: {result['content']}")
        print()

# Run the test
test_vector_search()



