import os
import pymongo
import streamlit as st
import pandas as pd
import folium
import re
import requests
import gc
import api_ninja
import mongodb
import plotly.express as px
import pydeck as pdk
from folium.plugins import MarkerCluster
from sentence_transformers import SentenceTransformer
from huggingface_hub import InferenceClient
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu
from mongodb import collection
from mongodb import vector_search
from folium.plugins import FastMarkerCluster
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
from streamlit_folium import folium_static


    # Nel main sono presenti le funzioni usate per le sezioni "Analitiche", "Filtro di ricerca" e "Q&A"
    # Prestare attenzione al path dell'immagine del logo e del file relativo alla latitudine e alla longitudine, per avere
    # un corretto funzionamento.
    
    # Per l'avvio: streamlit run main.py

    # All'avvio viene eseguita la parte di codice presente in mongodb.py




#---------- Funzioni per le analitiche e il filtro di ricerca ----------

#Filtro di ricerca dottori per specializzazione
def search_for_specialization():

    st.subheader("Ricerca dottori in base alla specializzazione")
    pipeline_specializations_doctors = [
        {
            "$match": {
                "Specialization": {"$type": "string"},
                "Doctor": {"$type": "string"}
            }
        },
        {
            "$project": {
                "Specialization": {"$split": ["$Specialization", ", "]},
                "Doctor": 1,
                "Location": 1
            }
        },
        {
            "$unwind": "$Specialization"
        },
        {
            "$group": {
                "_id": "$Specialization",
                "Doctors": {"$addToSet": {"Doctor": "$Doctor", "Location": "$Location"}}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    specializations_doctors = list(collection.aggregate(pipeline_specializations_doctors))
    df = pd.DataFrame(specializations_doctors)

    # Rimuovi punti e virgole dalle specializzazioni
    df['_id'] = df['_id'].apply(lambda x: x.replace('.', '').replace(',', '').replace(')', '').strip())

    # Menu a tendina per selezionare una specializzazione
    selected_specialization = st.selectbox("Seleziona una specializzazione", df['_id'].unique())

    # Filtra i dottori in base alla specializzazione selezionata
    selected_doctors = df[df['_id'] == selected_specialization]['Doctors'].values[0]

    doctors_df = pd.DataFrame(selected_doctors)
    st.title("Dottori per Specializzazione")
    st.dataframe(doctors_df)


#Distribuzione per piattaforme
def distribution_by_platform():

    st.subheader("Distribuzione per piattaforma")

    pipeline = [
        {   #L'alternativa è realizzarla prendendo gli url e filtrandoli lato applicazione
            "$project": {
                "platform": {
                    "$cond": [  # Il cond corrisponde ad un (If-then-else) | regexMatch è usato per il confronto in combo con $cond
                        {"$regexMatch": {"input": "$URL", "regex": "dica33"}}, "Dica33", #Se URL=dica33 ritorna dica33
                        {"$cond": [
                            {"$regexMatch": {"input": "$URL", "regex": "medicitalia"}}, "Medicitalia", #Se URL=medicitalia ritorna Medicitalia
                            {"$cond": [
                                {"$regexMatch": {"input": "$URL", "regex": "miodottore"}}, "MioDottore", #Se URL=miodottore ritorna miodottore
                                "Altri"
                            ]}
                        ]}
                    ]
                }
            }
        },
        {
            "$group": { #Raggruppa su platform e conta (Dica33|Medicitalia|MioDottore)
                "_id": "$platform",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}  # Ordina per conteggio decrescente
        }
    ]

    results = list(collection.aggregate(pipeline))

    # Convertiamo i risultati in un DataFrame
    data = pd.DataFrame(results)
    data.rename(columns={"_id": "Piattaforma", "count": "Conteggio"}, inplace=True)


    st.write("Distribuzione per piattaforma:")
    st.dataframe(data)
    st.bar_chart(data.set_index("Piattaforma")["Conteggio"])

#Distribuzione dottori per locazione senza mappa interattiva (usata per il testing iniziale, guardare count_doctors_by_location)
def count_doctors_by_location_without_map():

    st.subheader("Contare i Dottori per Locazione")

    # Pipeline per ottenere il numero di dottori per location
    pipeline = [
        {
            "$match": {
                "Location": {
                    "$ne": None,  # Esclude valori nulli
                    "$type": "string"  # Considera solo i valori di tipo stringa (escludo NaN)
                }
            }
        },
        {
            "$group": {
                "_id": "$Location",
                "doctorCount": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "doctorCount": -1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    # Creazione del DataFrame
    df = pd.DataFrame(results)
    df.columns = ["Location", "Doctor Count"]

    df.rename(columns={"_id": "Città", "doctorCount": "Numero Medici"}, inplace=True)


    st.write("Numero dottori per città:")
    st.dataframe(df)
    st.title("Distribuzione dei Dottori per Location")
    st.bar_chart(df.set_index("Location"))


#Distrubzione dei dottori per locazione con visualizzazione su mappa interattiva
def count_doctors_by_location():

    st.subheader("Contare i Dottori per Locazione")


    pipeline = [
        {
            "$match": {
                "Location": {
                    "$ne": None,  # Esclude valori nulli
                    "$type": "string"  # Considera solo i valori di tipo stringa (escludo NaN)
                }
            }
        },
        {
            "$group": {
                "_id": "$Location",
                "doctorCount": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                "doctorCount": -1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    df = pd.DataFrame(results)
    df.rename(columns={"_id": "Città", "doctorCount": "Numero Medici"}, inplace=True)

    # Visualizzazione del DataFrame
    st.write("Numero dottori per città:")
    st.dataframe(df)
    st.title("Distribuzione dei Dottori per Location")
    st.bar_chart(df.set_index("Città"))

    # Caricamento File Excel dal path apposito
    coordinates_df = pd.read_excel("lat_long.xlsx")

    # Unione dei dati dei dottori con le coordinate
    merged_df = pd.merge(df, coordinates_df, on="Città", how="left")

    # Selettore per il numero massimo di città
    max_cities = st.slider("Numero massimo di città da visualizzare", 1, min(len(merged_df), 500), 20)
    filtered_df = merged_df.head(max_cities)

    # Se voglio ridurre il carico dei marker
    #merged_df = merged_df.head(20)

    # Creazione della mappa (Si usa folium)
    mappa = folium.Map(location=[41.8719, 12.5674], zoom_start=6)

    # Creazione di un MarkerCluster
    marker_cluster = MarkerCluster().add_to(mappa)

    # Ciclo per aggiungere i marker
    for index, row in filtered_df.iterrows():
            lat = row['Latitudine']
            lon = row['Longitudine']

            # Aggiungi il marker al cluster
            folium.Marker(
                location=[lat, lon],
                popup=f"{row['Città']}: {row['Numero Medici']} medici",
                icon=folium.Icon(icon="info-sign")
            ).add_to(mappa)


    st.title("Mappa dei Dottori per Città")
    folium_static(mappa)


#Distribuzione delle specializzazioni
def distribution_by_specialization():

    pipeline = [
        {
            "$match": {
                "Specialization": {"$type": "string"}
            }
        },
        {
            "$project": {
                "Specialization": {
                    "$split": ["$Specialization", ", "]  # Divide le specializzazioni in una lista
                }
            }
        },
        {
            "$unwind": "$Specialization"  # Espande la lista in documenti separati
        },
        {
            "$group": {
                "_id": "$Specialization",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]

    data = list(collection.aggregate(pipeline))
    df = pd.DataFrame(data).rename(columns={"_id": "Specializzazione", "count": "Conteggio"})

    # Rimuovi punti e virgole dalle specializzazioni
    df['Specializzazione'] = df['Specializzazione'].apply(lambda x: x.replace('.', '').replace(',', '').replace(')', '').strip())

    st.dataframe(df)
    st.bar_chart(df.set_index("Specializzazione")["Conteggio"])



#Analisi delle domande per anno e mese
def search_year():
    st.subheader("Analisi delle Domande per Anno e Mese")

    pipeline_questions_per_year = [
        {
            "$match": {
                "Question Date": {"$type": "string"}
            }
        },
        {
            "$project": {
                "year": {"$substr": ["$Question Date", 0, 4]},  # Estrae l'anno
                "month": {"$substr": ["$Question Date", 5, 2]}  # Estrae il mese
            }
        },
        {
            "$group": {
                "_id": {"year": "$year", "month": "$month"},  # Raggruppa per anno e mese
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id.year": 1, "_id.month": 1}
        }
    ]

    result = list(collection.aggregate(pipeline_questions_per_year))

    data = [
        {   # Dividi L'id per l'analisi
            "Year": item["_id"]["year"],
            "Month": item["_id"]["month"],
            "Questions Count": item["count"]
        }
        for item in result
    ]

    df = pd.DataFrame(data)

    # Selettori per filtrare i dati
    selected_year = st.selectbox("Seleziona un anno", df["Year"].unique())
    filtered_df = df[df["Year"] == selected_year]

    # Visualizza i dati filtrati
    st.write(f"Numero di domande per mese nell'anno {selected_year}")
    st.dataframe(filtered_df)

    # Grafico delle domande per mese
    st.bar_chart(data=filtered_df.set_index("Month")["Questions Count"])


#----------------------------------------------------------------------------








# ------------------- Gestione Delle Pagine--------------------------


def homepage():
    """

    Mostra la home page con un messaggio di benvenuto e un'immagine.

    """
    st.title("Benvenuto nel Sistema di Q&A Medico!")
    image_path = r"D:\Users\Desktop\BigData_Proj\progetto\logo\q&a.png" #Modificare il Path se necessario
    st.image(image_path, use_container_width=True)

def qa_section():
    """

    Sezione Q&A: consente agli utenti di porre domande e ricevere risposte.
    """
    st.subheader("Sezione Q&A")
    query = st.text_input("Domanda:", placeholder="Inserisci la tua domanda qui...")

    # Messaggio di attesa
    message_placeholder = st.empty()

    os.environ["HF_TOKEN"] = "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"
    llm = InferenceClient(
        "mistralai/Mixtral-8x7B-Instruct-v0.1",
        token=os.getenv("HF_TOKEN")
    )

    if st.button("Ottieni Risposta"):
        if query:
            context_docs = vector_search(query, collection)
            prompt = f"""
            [INST]
            You are a doctor who answers only medical questions in a compassionate, understanding, and empathetic manner. 
            Use the following Context to provide a clear, accurate, and concise answer to the question. Do not simply copy the information from the {context_docs}. 
            Instead, rephrase or summarize the information in your own words, making it easier for the user to understand. 
            If the information is not present in the {context_docs}, respond with: "Non posso aiutarla con questo problema." 
            If the question is not related to a medical issue, say: "Non posso rispondere a questa domanda." 
            
            
            Context: {context_docs}
            Question: {query}
            Answer: 
            The answer must be in Italian. 
            Only provide answers based on the information in the {context_docs}. If the context does not answer the question, do not create any speculative or imaginative content.
            Start with "Gentile Utente," followed by a well-structured and conversational answer. 
            Make the response empathetic and human, demonstrating care and understanding for the user's situation. Use varied sentence structures and vocabulary to keep the response engaging and fluid. 
            Explain medical concepts in a way that is accessible and comforting, avoiding a purely technical tone. 
            Encourage the user to seek further support if needed, emphasizing the importance of taking care of oneself. 
            End with "Cordiali Saluti.
            
            [/INST]
            """
            message_placeholder.text("Sto elaborando la tua domanda...")
            output = llm.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.4,
                top_p=0.9,
            )
            response = output.choices[0].message.content
            message_placeholder.empty()
            st.write(response)
        else:
            st.warning("Per favore, inserisci una domanda.")

def analytics_section():
    """

    Sezione Analytics: mostra le statistiche sui dati.

    """
    st.subheader("Sezione Analytics")
    analytics_options = [
        "Distribuzione per specializzazione",
        "Distribuzione dottori per locazione con mappa",
        "Distribuzione per piattaforma"
    ]
    selected_analytic = st.selectbox("Scegli un'analitica:", analytics_options)

    if selected_analytic == "Distribuzione per specializzazione":
        distribution_by_specialization()

    elif selected_analytic == "Distribuzione dottori per locazione con mappa":
        count_doctors_by_location()

    elif selected_analytic == "Distribuzione per piattaforma":
        distribution_by_platform()

def search_section():
    st.subheader("Filtro di ricerca")
    filter_options = [
        "Ricerca dottori per specializzazioni",
        "Analisi delle Domande per Anno e Mese"
    ]
    selected_analytic = st.selectbox("Scegli filtro di ricerca:", filter_options)

    if selected_analytic == "Ricerca dottori per specializzazioni":
        search_for_specialization()

    elif selected_analytic == "Analisi delle Domande per Anno e Mese":
        search_year()


#---------------------------------------------------------------------------------





#-------------------- Menù e Navigazione -----------------------------------------

# Controllo della navigazione | Inizializzazione stato della sessione
if 'navigato' not in st.session_state:
    st.session_state.navigato = False

# Check stato sessione
if not st.session_state.navigato:
    homepage()
    if st.button("Vai al menu"):
        st.session_state.navigato = True
        st.rerun() # Fix per il doppio click in home

else:
# Menu di navigazione
    selected = option_menu(
        menu_title="Menù",
        options=["Q&A", "Analytics", "Filtro di ricerca"],
        icons=["question-circle", "bar-chart"],
        menu_icon="cast",
        default_index=0, # Evita che visualizzi altri elementi
        orientation="horizontal",
    )
    if selected == "Q&A":
        qa_section()
    elif selected == "Analytics":
        analytics_section()
    elif selected == "Filtro di ricerca":
        search_section()






