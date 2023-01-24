from .bajada_datos_subcat import main_data_download
from .clasificacion import main_classifier
from .procesamiento_old import main_processor
from .sustantivos_old import main_sustantivos

def main() -> None:
    # Descarga las trends del día:
    main_data_download()
    
    # Clasifica las trends del día:
    main_classifier()
    
    # Falta el main de procesamiento.py
    main_processor()
    
    # Falta el main de sustantivos.py
    main_sustantivos()


if __name__ == '__main__':
    main()
    