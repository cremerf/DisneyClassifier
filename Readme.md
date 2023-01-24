# Disney - MELI API Project

Este proyecto, desarrollado por **Intellignos | Havas**, extrae de forma diaria, desde la API de Mercado Libre, las keywords relativas a las tendencias de cada día, desagregadas por categorías y sub-categorías.

## Requirements

Instalar los módulos necesarios que se encuentran dentro del archivo requirements.txt
Dentro de la carpeta del proyecto, correr el comando:

```bash
 pip install -r /requirements.txt
```

## Arquitectura del proyecto
### Módulos y Carpetas:

#### Carpetas de datos:
```python
Proyecto
|_ Trends_Cat # Contiene las carpetas donde se almacenan los archivos diarios generados por cada script.
|  |_ bajada_datos_diaria # Contiene los archivos generados cada día por el script bajada_datos.py
|  |_ clasificacion_diaria # Contiene los archivos generados cada día por el script clasificacion.py
|  |_ procesamiento_diaria # Contiene los archivos generados cada día por el script procesamiento.py
|  |_ sustantivos_diaria # Contiene los archivos generados cada día por el script sustantivos.py
|  |
|  |_ df_cat_subcat.xlsx  # Contiene una tabla con todas las categorías y subcategorías y sus códigos.
|  |_ df_final_subcategorias.xlsx  # Contiene una tabla con todas las extracciones diarias de keywords realizada por Globant. (Utilizar para desagregar las keywords día por día)
|
|_ datos # Contiene los archivos de Personajes y Marcas sobre los cuales evaluar las coincidencias.
   |_ Personajes.xlsx
   |_ marcas_final.xlsx
```

#### Módulos
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



## Developed by
**Intellignos | Havas**\
Website: (https://intellignos.com/es/)
