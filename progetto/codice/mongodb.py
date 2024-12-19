import os
import pymongo
import streamlit as st
import pandas as pd
import folium
import re
import requests
import gc
from folium.plugins import MarkerCluster
from sentence_transformers import SentenceTransformer
from huggingface_hub import InferenceClient
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu


#In questa sezione è presente il codice relativo alla connessione di MongoDB
#Per l'embedding del dataset si deve fare riferimento al file relativo a google colab presente nella cartella.




# ------------ Funzioni per MongoDB --------------------------

def get_mongo_client(mongo_uri):
    """
    Stabilisce una connessione al database MongoDB.

    """
    try:
        client = pymongo.MongoClient(mongo_uri)
        print("Connection to MongoDB successful")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Connection failed: {e}")
        return None

# Funzione embedding
def get_embedding(text: str) -> list[float]:
    """
    Genera l'embedding per un determinato testo utilizzando SentenceTransformer.

    """
    if not text.strip():
        print("Attempted to get embedding for empty text.")
        return []

    embedding = embedding_model.encode(text)
    return embedding.tolist()

# Funzione di ricerca vettoriale
def vector_search(user_query, collection):
    """
    Esegue una ricerca vettoriale nella collezione MongoDB basata sulla query utente.

    """
    query_embedding = get_embedding(user_query)

    if query_embedding is None or not isinstance(query_embedding, list):
        print("Invalid query or embedding generation failed.")
        return []

    # Nota: Il numero di candidati può migliorare la qualità del risultato anche se probabilmente 150 già sono troppi considerando la dimensione del dataset
    # Aumentare i candidati aumenta anche il tempo di elaborazione

    pipeline = [
        {
            "$vectorSearch": {
                "index": "vector_index",  # Nome dell'indice
                "path": "embedding",  # Campo contenente gli embedding
                "queryVector": query_embedding,  # Embedding della query
                "numCandidates": 150,  # Numero di candidati da considerare
                "limit": 10,  # Limite dei risultati
            }
        },
        {
            "$project": {
                "_id": 0,  # Escludi il campo _id
                "Question": 1,
                "Answer": 1,
                #"Doctor": 1,
                #"Specialization": 1,
                #"URL": 1,
                "score": {"$meta": "vectorSearchScore"}  # Score di similarità
            }
        }
    ]
    results = collection.aggregate(pipeline)
    return list(results)

#-------------------------------------------------------------




# ------------------- Connessione a MongoDB -------------------------
mongo_uri = "mongodb+srv://antimo:esame123@bigdata.nuu2w.mongodb.net/?retryWrites=true&w=majority&appName=BigData"
embedding_model = SentenceTransformer("thenlper/gte-large")

if 'mongo_client' not in st.session_state:
    mongo_client = get_mongo_client(mongo_uri)
    st.session_state.mongo_client = mongo_client
else:
    mongo_client = st.session_state.mongo_client

db = mongo_client['Project']
collection = db['Q&A']
#--------------------------------------------------------------------