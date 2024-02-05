import os
import json
from fastapi import APIRouter
import pandas as pd
import gzip 

# Obtén la ruta al directorio 'models'
models_directory = os.path.join(os.path.dirname(__file__), '../models')

router = APIRouter()

@router.get("/developer/{developer}")
def developer(developer : str):
    ruta_archivo_gz = os.path.join(models_directory, 'steam_games.json.gz')
    
    try:
        # Abrir el archivo comprimido y leer su contenido descomprimiéndolo
        with gzip.open(ruta_archivo_gz, 'rt') as archivo:
            df = pd.read_json(archivo, lines=True)
       

        # Eliminar filas con fechas NaT (Not a Time)
        df = df.dropna(subset=['release_date'])
        
        # Filtrar registros con fechas incorrectas
        df = df[df['release_date'].str.match(r'\d{4}-\d{2}-\d{2}$')]

        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

        # Filtrar el dataframe por el publisher proporcionado
        df_publisher = df[df['developer'] == developer].copy(deep=True)

        # Crear una columna adicional para indicar si el contenido es gratis
        df_publisher['es_gratis'] = df_publisher['price'].isin(['Free to Use', 'Free to Play'])

        # Agrupar por año y contar la cantidad de items y el porcentaje de contenido gratis
        resultado = df_publisher.groupby(df_publisher['release_date'].dt.year)['es_gratis'].agg(['count', 'mean']).reset_index()

        resultado['mean'] = (resultado['mean'] * 100).round(2)

        resultado = resultado.rename(columns={'release_date': 'year', 'count': 'number_of_items', 'mean': 'free_content'})
        
        return convertir_dataframe_a_json(resultado)

    except Exception as e:
        # Manejo de la excepción
        print(f"Ocurrió una excepción: {e}")
        return None

def convertir_dataframe_a_json(df):
    dataframe_dict = df.to_dict(orient='records')
    dataframe_json = json.dumps(dataframe_dict, indent=2)
    dataframe_json_sin_formato = ''.join(dataframe_json.split())
    dataframe_objeto = json.loads(dataframe_json_sin_formato)
    return dataframe_objeto
