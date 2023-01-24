import pandas as pd
import numpy as np
from datetime import datetime
from packages.disney_paths import DisneyPaths
from packages.tools import Tool


# NOTE: Creamos un string "DATE_PKL" que tiene la fecha de hoy 'YYYY-MM-DD' + la extensión .pkl para agregar al path donde guardar el archivo final

#DATE_PKL = f"{str(datetime.today().date())}.pkl"

DATE = str(datetime.today().date()).replace('-', '_') # ejemplo: 2022_04_13 ('%Y-%m-&d')

# NOTE: Creamos la instancia de los Paths:
PATHS = DisneyPaths(date=DATE)

# NOTE: Creamos la instancia de Tool, que tiene las funciones de limpieza que no hacen al código en sí:

TOOL = Tool()

    #NOTE: PERSONAJES:

def create_array_personajes_clean() -> np.ndarray:  

    # #: Leemos desde el PATHS el excel de Personajes

    df_personajes = TOOL.open_file_xlsx_csv_pkl(PATHS.PERSONAJES_FILE_XLSX)

    # #: Eliminamos lo que contiene 'eliminar' y 'Eliminar' de la columna 'Filtrar
    df_personajes = df_personajes[(df_personajes.Filtrar != 'eliminar') & (df_personajes.Filtrar != 'Eliminar')]

    columnas_1 = ['Grupo/Empresa/owner derechos', 'FRANQUICIA','Sub-Franquicia', 'Contenido','PERSONAJE']

    # #: Se lo reasignamos a df_personajes
    df_personajes = df_personajes[columnas_1]
    df_personajes = df_personajes.apply(lambda x: x.str.lower())

    # #: asignamos un serie con los valores únicos de las personajes para poder usar el apply
    personajes_gral = pd.Series(df_personajes.PERSONAJE.unique())

    # #: creamos un array limpio de todos los personajes: en minúscula y sin símbolos
    arr_personajes_gral_clean = personajes_gral.apply(lambda x: TOOL.preprocess_clean_text(x)).to_numpy()

    return arr_personajes_gral_clean

    #NOTE: MARCAS:
def create_array_marcas_clean() -> np.ndarray:
    # #: asignamos un serie con los valores únicos de las marcas para poder usar el apply
    df_marcas = TOOL.open_file_xlsx_csv_pkl(PATHS.MARCAS_FILE_XLSX)
    marcas_gral = pd.Series(df_marcas.Marca.unique())

    # #: creamos un array limpio de todas las marcas: en minúscula y sin símbolos
    arr_marcas_gral_clean = marcas_gral.apply(lambda x: TOOL.preprocess_clean_text(x)).to_numpy()

    return arr_marcas_gral_clean

    #NOTE: TRENDS:
def create_trends_DataFrame_categorized(arr_personajes_gral_clean, arr_marcas_gral_clean) -> pd.DataFrame:
    
    df_trends = TOOL.open_file_xlsx_csv_pkl(PATHS.FINAL_TRENDS_SUBCAT_FILE_CSV)
    columns = ['Keyword', 'URL', 'Fecha', 'Pais', 'Categoria', 'Subcategoria']
    #df_trends = df_trends[columns]
    df_trends = df_trends.loc[:,columns]
    df_trends.drop_duplicates(inplace=True)
    df_trends.fillna('', inplace=True) # Hacemos esto por si alguna keywords es NaN (por algún motivo pasó)
    
    # #: Limpiamos y pasamos a minúscula todas las keywords
    df_trends.Keyword.astype(str).apply(lambda x: TOOL.preprocess_clean_text(str(x)))
    
    # #: Creamos la columna Personaje y la columna Marca dentro del Dataframe de Tendencias (df_trends)
    df_trends['Personaje'] = pd.Series(dtype=object)
    df_trends['Marca'] = pd.Series(dtype=object)

    print("Comienza la clasificación x personaje.\n")
    # #: Hacemos un apply s/las columnas Personaje y Marcas, para que matchee con cada keyword y encuentre si hay alguna coincidencia.
    # #: Si la hay, agrega el personaje y/o marca en la fila correspondiente.
    df_trends['Personaje'] = df_trends['Keyword'].apply(lambda keyword: TOOL.get_word_from_array(arr_personajes_gral_clean,keyword))
    print("Comienza la clasificación x marca.\n")
    df_trends['Marca'] = df_trends['Keyword'].apply(lambda keyword: TOOL.get_word_from_array(arr_marcas_gral_clean,keyword))

    return df_trends

def export_categorized_DF_to_csv(df_trends) -> None:
    #df_trends.to_pickle(PATH_TRENDS_DIARIAS_PKL)
    df_trends.to_csv(PATHS.SAVED_TRENDS,index=False,encoding='utf-8 sig', sep=";")
    

    #TODO: CREAR UN METODO QUE CREE EL EXCEL POR SI LO TIENE QUE ABRIR Y USAR UN USUARIO AJENO A ESTE SCRIPT:
    #df_trends.to_excel('./df_trends_diarias_excel.xlsx')

def main_classifier():
    print("RUNNING...")
    
    arr_personajes_gral_clean = create_array_personajes_clean()
    arr_marcas_gral_clean = create_array_marcas_clean()
    
    df_trends = create_trends_DataFrame_categorized(arr_personajes_gral_clean=arr_personajes_gral_clean,arr_marcas_gral_clean=arr_marcas_gral_clean)
    
    export_categorized_DF_to_csv(df_trends)
    
    print("CLASIFICACION COMPLETA.")


if __name__ == '__main__':
    main_classifier()