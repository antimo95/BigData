import requests
import re
import pandas as pd
import requests

"""
    Queste funzioni sono state sfruttate per creare un dataset sulle città presenti nella base rag, anche se il loro 
    utilizzo non è più presente nel main per rendere più pulito e leggibile il codice.
    Il file excel di riferimento per l'associazione della città a latitudine e longitudine è "lat_long.xlsx" 
    La funzione di clean_city_name è stata usata per uniformare i nomi delle città di questo dataset a quello
    della base rag poiché si aveva problemi col merge, poiché quando si è testata la funzione in ordine, non si 
    era effettuata la normalizzazione dei dati delle città in una prima fase.
    In alternativa si sarebbe potuto usare spark per raggruppare le città uguali, poiché mongoDB è case-sensitive quindi
    valori come Torino/TORINO vengono considerati come valori distinti mentre spark può essere configurato per raggruppare
    valori di questo genere.
    
    La query + parte applicata per l'operazione è riportata a fondo pagina
"""


# Funzione per ottenere latitudine e longitudine per città usando l'API NINJAS
def get_coordinates(nome_citta):
    api_url = 'https://api.api-ninjas.com/v1/geocoding?city={}'.format(nome_citta)
    response = requests.get(api_url, headers={'X-Api-Key': 'LPDPtjZNFC6UoDMJlZ9BBw==MNELlJquc4hFM3j3'})

    # Verifica che la risposta sia corretta
    if response.status_code == requests.codes.ok:
        data = response.json()  # Estrai la risposta JSON
        if data:
            lat = data[0]['latitude']  # Estrai latitudine
            lon = data[0]['longitude']  # Estrai longitudine
            return lat, lon
    return None, None


# Funzione per pulire i nomi delle città (rimuovere testo tra parentesi e spazi superflui)
def clean_city_name(city):
    """Rimuove il testo tra parentesi e gli spazi superflui senza cambiare maiuscole/minuscole."""
    city = re.sub(r"\s*\([^)]*\)", "", city).strip()
    city = city.strip().lower()
    return city  # Non modifica il caso delle lettere

# Query modificata per estrarre le città e salvare latitudine e longitudine su un file excel
def count_doctors_by_location_save_excel():
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
    df.rename(columns={"_id": "Città", "doctorCount": "Numero Medici"}, inplace=True)

    # Visualizzazione del DataFrame
    st.write("Numero dottori per città:")
    st.dataframe(df)

    # Visualizzazione con Streamlit
    st.title("Distribuzione dei Dottori per Location")
    st.bar_chart(df.set_index("Città"))

    # Caricamento File Excel dal path apposito
    coordinates_df = pd.read_excel("lat_long.xlsx")

    # Unione dei dati dei dottori con le coordinate
    merged_df = pd.merge(df, coordinates_df, on="Città", how="left")

    # Slider per selezionare il numero massimo di città
    max_cities = st.slider("Numero massimo di città da visualizzare", 1, min(len(merged_df), 500), 20)
    filtered_df = merged_df.head(max_cities)

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
        ).add_to(marker_cluster)

    # Visualizzazione della mappa in Streamlit con un key unico per evitare sovrapposizioni
    st.title("Mappa dei Dottori per Città")
    folium_static(mappa)

    # Ottieni le coordinate per ogni città e aggiungile al DataFrame
    merged_df['Latitudine'], merged_df['Longitudine'] = zip(*merged_df['Città'].apply(lambda loc: get_coordinates(clean_city_name(loc))))

    # Salva il DataFrame con le coordinate su un file Excel
    merged_df.to_excel("map_location2.xlsx", index=False)
    st.success("Il mapping delle location è stato salvato su 'map_location.xlsx'.")