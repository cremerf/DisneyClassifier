# Disney - MELI Classifier Project

This Project makes daily downloads of data (keywords) of every endpoint from MercadoLibreAPI (https://developers.mercadolibre.com.ar/), then, proceprocess data (data cleaning & data wrangling), make some validations and classifies with Regex module each keyword regarding brands, characters, saga, movies and franchise from The Walt Disney Company Universe. 

Finally, each classified keyword stores in a dataframe. 

## Requirements


Install requirements with the following command

```bash
 pip install -r /requirements.txt
```

## Root folder
### Modules & folders:

#### Data Folder:
```python
Proyecto
|_ Trends_Cat # Folder where daily files generated from each script stores.
|  |_ bajada_datos_diaria # Stores generated daily files from bajada_datos.py
|  |_ clasificacion_diaria # Stores generated daily files from clasificacion.py.
|  |_ procesamiento_diaria # Stores generated daily files from procesamiento.py
|  |_ sustantivos_diaria # Stores generated daily files from sustantivos.py.
|  |
|  |_ df_cat_subcat.xlsx  # Stores df with every category, subcategory and codes from API
|  |_ df_final_subcategorias.xlsx  # Stores df with downloaded keywords
|
|_ datos # Stores data from TWDC (Brands & Characters)
   |_ Personajes.xlsx
   |_ marcas_final.xlsx
```

#### Modules
```python
|_ packages # Contiene módulos desarrollados con clases (POO) utilizados como herramientas a lo largo del código, del mismo modo que un módulo para almacenar los directorios en un único lugar.
|  |_ tools.py # Contiene la Clase Tool que posee métodos de clasificación y preprocesamiento para ser invocados dentro del script necesario y evitar repetir código.
|  |_ disney_paths.py # Contiene la clase DisneyPath, la cual almacena los distintos directorios de subida, guardado o directamente de archivos para que sea más facil su administración y escalabilidad del código.
|  |_ globant_aws.py # Este módulo fue pensado para trabajar en conjunto con Globant en lo relativo al Deploy, donde ellos pudieran de forma aislada trabajar sus propios métodos para realizar el deploy, sin la necesidad de tener que retocar el código original.
|
|
|_ bajada_datos_subcat.py # Este módulo se encarga de realizar la extracción de las keywords de cada día desde la API de MELI, obteniendo como resultado un Dataframe exportado como CSV bajo el nombre/formato: "YYYY/MM/DD_trends_diarias_subcat.csv", con las columnas "Keyword", "URL", "Fecha", "País", "Categoría" y "Subcategoría".
|_ clasificacion.py # Este módulo realiza la primera clasificación a gran escala de las keywords, obteniendo como resultado un Dataframe exportado como CSV bajo el nombre/formato: "YYYY/MM/DD_trends_clasificadas.csv".
| # Los siguientes módulos "procesamiento_old.py" y "sustantivos_old.py" no llegaron a ser refactorizados ni detallados línea por línea, siendo que se ocupaban de una clasificación con mayor detenimiento 
|_ procesamiento_old.py # Genera como resultado un Dataframe exportado como CSV bajo el nombre/formato: "YYYY/MM/DD_procesado_sinprod.csv".
|_ sustantivos_old.py # Genera como resultado un Dataframe exportado como CSV bajo el nombre/formato: "YYYY/MM/DD_procesado_diario.csv".
|
|_ main.py # Este script es el main del proyecto. Se encarga de correr todos los módulos en conjunto y de forma secuencial. Es el script que debe automatizarse. 

```



## Authors
**Federico Cremer | Andres De Innocentiis**\
