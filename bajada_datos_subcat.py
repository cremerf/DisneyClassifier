from types import NoneType
import requests
from datetime import date, datetime
import pandas as pd
from pandas import json_normalize, DataFrame
from packages.disney_paths import DisneyPaths
from packages.tools import Tool

from pathlib import Path
print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())

DATE = str(datetime.today().date()).replace('-', '_')
##: Instanciamos DisneyPaths y Tool
PATHS = DisneyPaths(date=DATE)
TOOL = Tool()


PAISES = {"Argentina":"MLA",
#"Bolivia":"MBO",
"Brasil":"MLB",
"Chile":"MLC",
"Colombia":"MCO",
#"Costa Rica":"MCR",
#"República Dominicana":"MRD",
#"Ecuador":"MEC",
#"Guatemala":"MGT",
#"Honduras":"MHN",
"Mexico":"MLM",
#"Nicaragua":"MNI",
#"Panamá":"MPA",
#"Paraguay":"MPY",
#"El Salvador":"MSV",
#"Uruguay":"MLU"
}



#acá homologamos las categorias ya que el nombre y código asociado varían según el país
DICT_CATEGORIAS_GRAL={'Accesorios para Vehiculos': ['MLA5725', 'MCO1747', 'MLM1747', 'MLB5672',"MLC1747"],
 'Agro': ['MLA1512', 'MCO441917', 'MLM189530', 'MLB271599',"MLC1512"],
 'Alimentos y Bebidas': ['MLA1403', 'MCO1403', 'MLM1403', 'MLB1403', "MLC1403"],
 'Animales y Mascotas': ['MLA1071', 'MCO1071', 'MLM1071', 'MLB1071',"MLC1071"],
 'Antiguedades y Colecciones': ['MLA1367', 'MCO1367', 'MLM1367', 'MLB1367',"MLC1367"],
 'Arte, Libreria y Merceria': ['MLA1368', 'MCO1368', 'MLM1368', 'MLB1368',"MLC1368"],
 'Autos, Motos y Otros': ['MLA1743', 'MCO1743', 'MLM1743', 'MLB1743',"MLC1743"],
 'Bebes': ['MLA1384', 'MCO1384', 'MLM1384', 'MLB1384',"MLC1384"],
 'Belleza y Cuidado Personal': ['MLA1246', 'MCO1246', 'MLM1246', 'MLB1246',"MLC1246"],
 'Camaras y Accesorios': ['MLA1039', 'MCO1039', 'MLM1039', 'MLB1039',"MLC1039"],
 'Celulares y Telefonos': ['MLA1051', 'MCO1051', 'MLM1051', 'MLB1051',"MLC1051"],
 'Computacion': ['MLA1648', 'MCO1648', 'MLM1648', 'MLB1648',"MLC1648"],
 'Consolas y Videojuegos': ['MLA1144', 'MCO1144', 'MLM1144', 'MLB1144',"MLC1144"],
 'Construccion': ['MLA1500', 'MCO172890', 'MLM1500', 'MLB1500',"MLC1500"],
 'Deportes y Fitness': ['MLA1276', 'MCO1276', 'MLM1276', 'MLB1276',"MLC1276"],
 'Electrodomesticos y Aires Ac.': ['MLA5726', 'MCO5726', 'MLM1575', 'MLB5726',"MLC5726"],
 'Electronica, Audio y Video': ['MLA1000', 'MCO1000', 'MLM1000', 'MLB1000', "MLC1000"],
 'Herramientas': ['MLA407134', 'MCO175794', 'MLM186863', 'MLB263532', "MLC178483"],
 'Hogar, Muebles y Jardin': ['MLA1574', 'MCO1574', 'MLM1574', 'MLB1574',"MLC1574"],
 'Industrias y Oficinas': ['MLA1499', 'MCO1499', 'MLM1499', 'MLB1499',"MLC1499"],
 'Inmuebles': ['MLA1459', 'MCO1459', 'MLM1459', 'MLB1459',"MLC1459"],
 'Instrumentos Musicales': ['MLA1182', 'MCO1182', 'MLM1182', 'MLB1182', "MLC1182"],
 'Joyas y Relojes': ['MLA3937', 'MCO3937', 'MLM3937', 'MLB3937',"MLC3937"],
 'Juegos y Juguetes': ['MLA1132', 'MCO1132', 'MLM1132', 'MLB1132',"MLC1132"],
 'Libros, Revistas y Comics': ['MLA3025', 'MCO3025', 'MLM3025', 'MLB3025', "MLC3025"],
 'Musica, Peliculas y Series': ['MLA1168', 'MCO1168', 'MLM1168', 'MLB1168',"MLC1168"],
 'Ropa y Accesorios': ['MLA1430', 'MCO1430', 'MLM1430', 'MLB1430', "MLC1430"],
 'Salud y Equipamiento Medico': ['MLA409431','MCO180800','MLM187772','MLB264586',"MLC409431"],
 'Servicios': ['MLA1540', 'MCO1540', 'MLM1540', 'MLB1540',"MLC1540"],
 'Souvenirs, Cotillon y Fiestas': ['MLA9304','MCO118204','MLM44011','MLB12404', "MLC435280"],
 'Otras categorias': ['MLA1953', 'MCO1953', 'MLM1953', 'MLB1953',"MLC1953"]}
       
                
def convert_JSONdata_to_DataFrame(data:list, cod_subcat:str, categoria:str, subcat:str) -> DataFrame:  # data es un json encoded, que se representa como una list.

    paises_nuevo = {value : key for (key, value) in PAISES.items()}

    df = json_normalize(data)

    df['Fecha'] = date.today()
    df['Pais'] = paises_nuevo[cod_subcat[:3]]
    df['Categoria'] = categoria
    df["Subcategoria"] = subcat
       
    return df     


def create_csv_from_DataFrame(df:DataFrame) -> None:

    filename = PATHS.FINAL_TRENDS_SUBCAT_FILE_CSV
    df.to_csv(filename, index = False, encoding = 'utf-8 sig', sep=";") 

def create_pkl_from_csv(csv_file):
    
    df = pd.read_csv(csv_file)
    pd.to_pickle(df, PATHS.FINAL_TRENDS_FILE_PKL)
    
    
def get_subcat_dict_from_file(filename_path:str) -> DataFrame:
    
    df = TOOL.open_file_xlsx_csv_pkl(PATHS.FILENAME_CAT_SUBCAT)
    
    df1 = df.loc[:,['Categoria_homologada','Subcategoria_homologada','Cod_Subcat']]
    df1 = df.loc[df.id_seleccionado == 1,['Categoria_homologada','Subcategoria_homologada','Cod_Subcat']]
    
    dict_subcat = {k: f.groupby('Subcategoria_homologada')['Cod_Subcat'].apply(list).to_dict() for k, f in df1.groupby('Categoria_homologada')}
   
    return dict_subcat 

def get_data_from_MELI_API(cod_subcat:str, categoria:str, subcat:str) -> pd.DataFrame:
    
    try:
        ##: 1) Hacemos una request con el código de país y el código de la categoría, nos trae de vuelta una response:
        link = f'https://api.mercadolibre.com/trends/{cod_subcat[:3]}/{cod_subcat}'
        response = requests.get(link)
        
        ##: 2) Esa response la convertimos a json:
        data = response.json()
        
        ##: 3) Si la respuesta no tuvo ningún error, convertimos la respuesta JSON en un DataFrame y le agragamos las columnas "Fecha, País y Categoría":
        if not 'error' in data:
            data_df = convert_JSONdata_to_DataFrame(data=data, cod_subcat=cod_subcat, categoria=categoria, subcat=subcat)
            return data_df
        else:
            pass
    except Exception as e:
        print(e)

    


# WARNING: Esto va a traer TODOS los datos cada vez que se ejecute.
def run() -> None:
    ##: RESUMEN: Por cada codigo de país, de cada categoría, hacemos una request, que nos trae data, la convertimos en JSON y ese JSON en un DataFrame. Finalmente con ese DataFrame creamos un archivo CSV y un archivo pickle
    df_data = []
    dict_subcat = get_subcat_dict_from_file(PATHS.FILENAME_CAT_SUBCAT)
    
    for categoria,subcats in dict_subcat.items():
        for subcat,codigos in subcats.items():
            for cod_subcat in codigos:
            
                df_data_codigo = get_data_from_MELI_API(cod_subcat=cod_subcat,categoria=categoria, subcat=subcat)
    
                if not type(df_data_codigo) is None:
                    df_data.append(df_data_codigo)

            
    df_data = pd.concat(df_data)
    
    column_names = ['Keyword','URL','Fecha','Pais','Categoria','Subcategoria']
    
    df_data.columns = column_names
    print(df_data)
    ##: 4) Acá abajo creamos el archivo csv en la carpeta Trends_Cat:            
    create_csv_from_DataFrame(df=df_data)
    
    ##: 5) Creamos un archivo pickle (.pkl) donde guardamos el DF de trends:
    # PAUSED: 
    #create_pkl_from_csv(PATHS.FINAL_TRENDS_FILE_CSV)
    
    print("Carga completa.")
    
    
            
def main_data_download():
    run()           
                 

if __name__ == '__main__':
    main_data_download()         
