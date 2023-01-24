import string
import re
import numpy as np
import pandas as pd
import os



#NOTE: Clase para crear herramientas a exportar y usar en el código.
class Tool():
    def __init__(self) -> None:
        pass
    
    ##: Convierte en minúscula, quita acentos a las palabras y quita signos de puntuacion:
    def preprocess_clean_text(self, word:str) -> str:
        # Pasamos a minúsculas
        clean_word = word.lower()
        
        # Quitamos caracteres de puntuacion:
        clean_word = ''.join([c for c in clean_word if c not in (string.punctuation+"¡"+"¿")])
        
        # Quitamos caracteres con acento:
        clean_word = re.sub(r'[áàâäãåāăą]','a', clean_word) 
        clean_word = re.sub(r'[ééèêëẽēėęěĕ]','e', clean_word) 
        clean_word = re.sub(r'[íìîïīįı]','i', clean_word) 
        clean_word = re.sub(r'[óòôöõøōő]','o', clean_word) 
        clean_word = re.sub(r'[úùûüūůűų]','u', clean_word) 

        return str(clean_word)

    
    def find_whole_word(self, word:str) -> str:
        
        match =  re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).search
        
        return match


    def object_to_string(self, personaje_o_marca:str,keyword:str) -> str or None:
        # Chequeamos si el personaje o la marca se encuentra contenido dentro de la keyword:
        match = self.find_whole_word(personaje_o_marca)(keyword)
        try:
            match = match.group()
        except:
            match = None
            
        return match
    
    
    def get_word_from_array(self, array:np.ndarray, keyword:str) -> str or float: # Puede devolver "float" xq asi se representa NaN
        
        list_of_matches = [word for word in array if self.object_to_string(word,keyword)]

        if len(list_of_matches) > 1:
            word_str = list_of_matches[0]
        else:
            if not list_of_matches:
                word_str = np.nan
            else:
                word_str = list_of_matches[0]

        return word_str
    
    
    
    
    ##: Este método detecta la extensión del archivo y lo abre con el método que corresponda a la extensión
    def open_file_xlsx_csv_pkl(self, path:str) -> pd.DataFrame:
        filename, file_extension = os.path.splitext(path)
        
        if file_extension == '.xlsx':
            file = pd.read_excel(path, engine='openpyxl')
            
        elif file_extension == '.csv':
            file = pd.read_csv(path, sep=";")
            
        elif file_extension == '.pkl':
            file = pd.read_pickle(path)
            
        return file
        
    
    

