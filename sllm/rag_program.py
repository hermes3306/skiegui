import ollama
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://bstyfs23:eZVn53ELtH7jGF4G@cluster0.uo0pbrc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['rag_db']
collection = db['documents']

# Initialize sentence transformer model
encoder = SentenceTransformer('all-MiniLM-L6-v2')

def add_document(text, metadata=None):
    # Encode the document
    embedding = encoder.encode(text)
    
    # Store the document and its embedding
    doc = {
        'text': text,
        'embedding': embedding.tolist(),
        'metadata': metadata or {}
    }
    collection.insert_one(doc)

def search_similar_documents(query, k=3):
    # Encode the query
    query_embedding = encoder.encode(query)
    
    # Search for similar documents
    results = collection.aggregate([
        {
            '$search': {
                'index': 'default',
                'knnBeta': {
                    'vector': query_embedding.tolist(),
                    'path': 'embedding',
                    'k': k
                }
            }
        }
    ])
    
    return list(results)

def generate_response(query, context):
    prompt = f"Context: {context}\n\nQuestion: {query}\n\nAnswer:"
    response = ollama.generate(model='llama2:7b', prompt=prompt)
    return response['response']

def rag_query(query):
    # Search for relevant documents
    similar_docs = search_similar_documents(query)
    
    # Prepare context from similar documents
    context = "\n".join([doc['text'] for doc in similar_docs])
    
    # Generate response using Llama 2
    response = generate_response(query, context)
    
    return response

# Example usage
add_document("The capital of France is Paris.")
add_document("The Eiffel Tower is located in Paris, France.")
add_document("Paris is known as the City of Light.")

query = "What is the capital of France and what is it known for?"
response = rag_query(query)
print(response)