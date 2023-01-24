from pathlib import Path
import os

#NOTE: Acá establecimos directorios absolutos para que solamente tengamos que tocar estas líneas de código en lo relativo a los imports.
class DisneyPaths():
    def __init__(self, date) -> None:
        self.date = date
        # Directorio de la carpeta del Proyecto:
        self.BASE_DIR = Path(__file__).resolve().parent.parent #Directorio base de la carpeta del proyecto. La que contiene a todos los archivos
        
        # Directorios de las carpetas que contienen los archivos a importar en el script
        self.IMPORT_FILES_DATOS_DIR = os.path.join(self.BASE_DIR, 'datos') # Directorio donde se encuentran los archivos xlsx a importar
        self.IMPORT_FILES_TRENDSCAT_DIR = os.path.join(self.BASE_DIR, 'Trends_Cat') # Directorio donde se encuentran los archivos csv a importar
        
        # Directorios de las carpetas donde se guardan archivos generados:
        self.BAJADA_DATOS_DIARIA_DIR = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'bajada_datos_diaria') # Path del directorio donde se van a guardar los archivos generados por bajada_datos.py
        self.CLASIFICACION_DIARIA_DIR = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'clasificacion_diaria') # Path del directorio donde se van a guardar los archivos generados por clasificacion.py
        self.PROCESAMIENTO_DIARIA_DIR = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'procesamiento_diaria') # Path del directorio donde se van a guardar los archivos generados por procesamiento.py
        self.SUSTANTIVOS_DIARIA_DIR = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'sustantivos_diaria') # Path del directorio donde se van a guardar los archivos generados por sustantivos.py
        self.SUSTANTIVOS_FINAL_PROCESADO_DIARIA_DIR = os.path.join(self.SUSTANTIVOS_DIARIA_DIR, 'final_procesado_diaria') # Path del directorio donde se van a guardar los archivos final procesado generados por sustantivos.py
        self.SUSTANTIVOS_METRICAS_NLP_DIARIA_DIR = os.path.join(self.SUSTANTIVOS_DIARIA_DIR, 'metricas_nlp_diaria') # Path del directorio donde se van a guardar los archivos métricas NLP generados por sustantivos.py
        
        
        # Directorios de los archivos a importar en el script
        # Provistos por Disney:
        self.MARCAS_FILE_XLSX = os.path.join(self.IMPORT_FILES_DATOS_DIR, 'marcas_final.xlsx') # El path al archivo marcas_final.xlsx
        self.PERSONAJES_FILE_XLSX = os.path.join(self.IMPORT_FILES_DATOS_DIR, 'Personajes.xlsx') # El path al archivo Personajes.xlsx 
        
        # Creados por los scripts:
        #TODO: AGREGAR RUTAS/PATHS DE ARCHIVOS DE GUARDADO AQUÍ:
        ##: Archivo que genera el módulo: bajada_datos.py:
        # WORKING! 
        # Archivo de Trends de Subcategorías diarias:
        self.FINAL_TRENDS_SUBCAT_FILE_CSV = os.path.join(self.BAJADA_DATOS_DIARIA_DIR, f'{self.date}_trends_diarias_subcat.csv')
        
        # DEPRECATED:  
        # Archivo de Trends de Categorías diarias:
        #self.FINAL_TRENDS_FILE_CSV = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, f'{self.date}_df_trends_diario.csv') # El path al archivo df_trends_diario.csv
        #self.FINAL_TRENDS_FILE_PKL = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'df_trends_diario.pkl') # El path al archivo df_final_trends1.xlsx  
        
        
        ##: Archivo que genera el módulo: clasificacion.py
        self.SAVED_TRENDS = os.path.join(self.CLASIFICACION_DIARIA_DIR, f'{self.date}_trends_clasificadas.csv') # Path del archivo final de Trends Diarias que genera el módulo clasificacion.py al que despues le sumamos la fecha y la extensión
        
        ##: PATH AL ARCHIVO GENERADO POR EL MÓDULO: procesamiento.py:
        self.FINAL_PROCESADO_SIN_PROD_CSV = os.path.join(self.PROCESAMIENTO_DIARIA_DIR, f'{self.date}_procesado_sinprod.csv')
    
        # PAUSED: 
        #self.FINAL_PROCESADO_SIN_PROD_PKL = os.path.join(self.IMPORT_FILES_DATOS_DIR, 'df_final_procesado_sinprod.pkl')
    
        ##: PATH A LOS ARCHIVOS GENERADOS POR EL MÓDULO: sustantivos.py:
        self.FINAL_PROCESADO_FILE_CSV = os.path.join(self.SUSTANTIVOS_FINAL_PROCESADO_DIARIA_DIR, f'{self.date}_procesado_diario.csv') # El path al archivo df_final_procesado.csv
        self.METRICAS_NLP_CSV = os.path.join(self.SUSTANTIVOS_METRICAS_NLP_DIARIA_DIR, f'{self.date}_metricas_NLP_diario.csv')
        # PAUSED: 
        #self.FINAL_PROCESADO_FILE_PKL = os.path.join(self.IMPORT_FILES_DATOS_DIR, 'df_final_procesado.pkl') # El path al archivo df_final_procesado.pkl
        
        
        ##: GLOBANT PATHS IN AWS:
        #IMPROVE: # PENDING GLOBANT
        self.FULL_LOCAL_INPUT_PATH = os.path.join(self.BASE_DIR, 'tmp')
        self.FILENAME_CAT_SUBCAT = os.path.join(self.IMPORT_FILES_TRENDSCAT_DIR, 'df_cat_subcat.xlsx')
        
        self.S3_BUCKET_NAME = os.path.join('dt-revenue-research-dev')
        self.PREFIX_S3_INPUT = os.path.join('mercado_libre/input/datos')
        self.PREFIX_S3_OUTPUT = os.path.join('mercado_libre/output/trend')
        self.DF_TO_APPEND_FILENAME = os.path.join(self.FULL_LOCAL_INPUT_PATH,"temp_df_subcat.csv")
        self.DF_RESULT_FILENAME = os.path.join('df_subcat.csv')