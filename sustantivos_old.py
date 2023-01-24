import numpy as np
import stanza
import pandas as pd
from datetime import datetime
import time
from nltk.tokenize import word_tokenize
import nltk
nltk.download('punkt')

from packages.disney_paths import DisneyPaths
from packages.tools import Tool


DATE = str(datetime.today().date()).replace('-', '_')

TOOL = Tool()
PATHS = DisneyPaths(date=DATE)

nlp_es = stanza.Pipeline('es',processors='tokenize,mwt,pos,lemma')
nlp_pt = stanza.Pipeline('pt',processors='tokenize,mwt,pos,lemma')

def run():
    def remove_quitar(data):
        new_text = ""
        words = word_tokenize(str(data))
        for w in words:
            if w not in quitar and len(w) > 1:
                new_text = new_text + " " + w
            
        return new_text.strip()

    def fix_word(palabra):
        palabra=str(palabra).lower()
        palabra=palabra.replace("á",'a')
        palabra=palabra.replace("é",'e')
        palabra=palabra.replace("í",'i')
        palabra=palabra.replace("ó",'o')
        palabra=palabra.replace("ú",'u')
        palabra=palabra.replace("ú",'u')
        palabra=palabra.replace(".",'')
        return palabra

    def lematizar_stanza(frase,lang='es'):
        
        if frase != np.nan:
            if lang =='es':
                doc = nlp_es(frase)
            elif lang =='pt':
                doc = nlp_pt(frase)
            else:
                raise Exception('Lenguaje no detectado')

            frase = ' '.join([word.lemma for sent in doc.sentences for word in sent.words])

        else:

            frase = np.nan
        
        return frase


    def filtrar(x,y):
        x = str(x)
        y = str(y)

        if x is None or y is None:
            return x
            
        x = x.split()
        for palabra in y.split():
            if palabra in x:
                x.remove(palabra)

        return ' '.join(x)


    def buscar_sustantivos_stanza(frase,lang='es'):

        if lang=='es':
            doc = nlp_es(frase)
        elif lang=='pt':
            doc = nlp_pt(frase)
        else:
            raise Exception('Lenguaje no detectado')
            
        sustantivos = [word.text for sent in doc.sentences for word in sent.words if word.upos=='NOUN']
        return ' '.join(sustantivos)


    df = TOOL.open_file_xlsx_csv_pkl(PATHS.FINAL_PROCESADO_SIN_PROD_CSV)


    df['Sust_Prueba'] = pd.Series(np.nan)
    df['Lematizado'] = pd.Series(np.nan)

    df.Keyword3 = df.Keyword3.astype(str)
    df.Keyword = df.Keyword.astype(str)
    df = df[df.Keyword.notnull()]



    df_filtrado_br = df[df['Pais']=='Brasil']

    #df_filtrado_br = df_filtrado_br.loc[:,df_filtrado_br.columns != 'Pais'].drop_duplicates()

    df_filtrado_br['Sustantivos'] = df_filtrado_br['Keyword3'].apply(buscar_sustantivos_stanza,args=('pt',))

    df_filtrado_resto = df[df['Pais'] != 'Brasil']
    #df_filtrado_resto = df_filtrado_resto.loc[:,df_filtrado_resto.columns != 'Pais'].drop_duplicates()
    df_filtrado_resto['Sustantivos'] = df_filtrado_resto['Keyword3'].apply(buscar_sustantivos_stanza,args=('es',))


    df_filtrado_br.loc[:,'Keyword3'] = df_filtrado_br['Keyword3'].astype(str)

    df_filtrado_resto.loc[:, 'Keyword3'] = df_filtrado_resto['Keyword3'].astype(str)

    df_filtrado_br['Sust_Lematizado']  = pd.Series(np.nan)

    df_filtrado_br['Lematizado'] = df_filtrado_br['Keyword3'].apply(lematizar_stanza,args=('pt',))
    df_filtrado_resto['Lematizado'] = df_filtrado_resto['Keyword3'].apply(lematizar_stanza,args=('es',))
    df_filtrado_br['Sust_Lematizado'] = df_filtrado_br['Sustantivos'].apply(lematizar_stanza,args=('pt',))
    df_filtrado_resto['Sust_Lematizado'] = df_filtrado_resto['Sustantivos'].apply(lematizar_stanza,args=('es',))



    df_filtrado = pd.concat([df_filtrado_br,df_filtrado_resto])

    df_filtrado_br['Lematizado'] = df_filtrado_br['Keyword3'].apply(lematizar_stanza,args=('pt',))
    df_filtrado_br['Sust_Lematizado'] = df_filtrado_br['Sustantivos'].apply(lematizar_stanza,args=('pt',))


    df_filtrado_resto['Lematizado'] = df_filtrado_resto['Keyword3'].apply(lematizar_stanza,args=('es',))
    df_filtrado_resto['Sust_Lematizado'] = df_filtrado_resto['Sustantivos'].apply(lematizar_stanza,args=('es',))

    df_filtrado = pd.concat([df_filtrado_br,df_filtrado_resto])

    quitar = ['rey leon','walt','maravillas','funco','pop','dragon','princesses','princesa',
    'rey','campanita tinker','homem','iron','capitan','captain','avenger','black','venzo',
    'airon','amazing','carnage','zombies','bruja','hombre arana','legends','medellin','espiderman',
    'toy biz','accion','select','funki pop','ps','if','iron','infinity saga','leggends','lellends',
    'legos','lego','legendds','mavel','select','x men','araña','spader','cachorros', 'monedas',
    'moneda pesos','aironman','twdc','thano','funoo','jumbao','pops','jasmine','noiva','tobey','galaxia','hombre','pato','scorpio',
    'descendiente','-man','mater','mujer','capitán','marbel','minie','monsters','relampago',
    'capital','infinity','mundo','ojo agamotto','rust eze','señor cara papa','super heroe','acción',
    'aranha','baby','cara papa','cosa dead pool','dalmata','dedpool','edición','far','ferro',
    'filosofia','funki','galaxis','gody','heroe','iroman','marvels','mecanico','mech','metal',
    'kunko','pez guppy','psicología walking dead','rayo','remix','rust eze pj','leyenda','leyends','lihna','mikey mause','mini',
    'minie niña','minni mouse','moano','mohana','monje','moral ultimate','nina','novedad','origen','pais','scarlet','simposon',
    'sofia','sr cara papa','stich','targeta labador','telarano','televisa','ultron','univers','villana','walking dead'
    ,'walking dead temporada','xiesta','nina','nino','niña','niño','toy biz','cosplay','arana','super',
    'man','men','death','hilux','hiluxx','toyota','bogota','diesel','corolla','stars','wars','war','death',
    'funko','potter','harry','beatle','lp','star','starwar','simpson','hammer','mcqueen','spider','ps'
    ,'wandavision','widow','gravity fall','princess' ,'pocahonta','clone hasbro','lighting mcqueen toy'
    ,'panther','car','mouse','wanda','rex','spaiderman','spider way home','xbox one','escorpion'
    ,'minni','ana','simpson','jedi','toy story','vengador','princess','mikey'
    ,'way home','buzz','scorpiom','doctora juguete','maravilla','beauty','legend','Nieve','pantera','ps',
    'home banking galicia','cruel','plus','dodge','vil','princesse','mattel','guardian','bell','blanco nieve','hasbro','inc','gw','loose',
    'devil','adida','aranho','pj','adults','deville','kombat','monster inc','viuda','chevrolet','princesita','monster','pirata caribe',
    'nieve','superhero','spederman','titan','adulto','samsung','canon','nikon','iphone','galaxy','honda','nintendo'
    'aironmar','homen','simpsom','tanos','america','luke','andrew','spaidermar','legendd','legende','lellend','hot toys','han','maximoff',
    'chebar','battlefront','casco fett','dead pool','aladino','diney','wini pooh','ariel','azoka tano','hom','dripper','toys story','univer','from',
    'clon','galicia banking','gema infinito','marvel','mickey','minnie','ray','micky mause','spiderman','disne',
    'aironmar','americo','vingador','spidey','mack','vader','cinderela','daredevil','mous','descendinte','anno','store',
    'espider','fantastico','fett','maus','secret','bestia','netflix','play','switch','gt / gtx','gt','gtx','gameboy']


    dfclas = TOOL.open_file_xlsx_csv_pkl(PATHS.PERSONAJES_FILE_XLSX)

    # #: Eliminamos lo que contiene 'eliminar' y 'Eliminar' de la columna 'Filtrar

    dfclas = dfclas[(dfclas.Filtrar != 'eliminar') & (dfclas.Filtrar != 'Eliminar')]

    columnas_1 = ['Grupo/Empresa/owner derechos', 'FRANQUICIA','Sub-Franquicia', 'Contenido','PERSONAJE']

    # #: Se lo reasignamos a dfclas

    dfclas = dfclas[columnas_1]
    dfclas = dfclas.apply(lambda x: x.str.lower())

    quitar2 = dfclas.PERSONAJE.unique()

    df_filtrado['Marca'] = df_filtrado['Marca'].str.lower()

    quitar3 = df_filtrado[~df_filtrado.Marca.isna()].Marca.unique()

    quitar = quitar + quitar2.tolist() + quitar3.tolist()

    pat = r'\b(?:{})\b'.format('|'.join(quitar))

    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Lematizado'].str.lower()
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.strip()
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.replace(pat, '')
    df_filtrado.loc[df_filtrado.Sust_Prueba == '','Sust_Prueba'] = np.NaN

    ## quita los numeros
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.replace('\d+', '')
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].apply(fix_word)

    ## quita letras sueltas
    df_filtrado.loc[:,'Sust_Prueba'] = df_filtrado.Sust_Prueba.str.replace(r'\b\w\b', '').str.replace(r'\s+', ' ')

    df_filtrado.drop_duplicates(inplace=True)
    df_filtrado.loc[df_filtrado.Sust_Prueba.isin(['nan','',' ']),'Sust_Prueba'] = np.NaN
    ## corrige errores ortografia
    df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('disfras',na=False)), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('disfras',na=False)), :].Sust_Prueba.replace(r'(?:\s|^)disfras(?:\s|$)','disfraz ', regex=True)
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.replace('beber','bebe')
    df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('disfrases',na=False)), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('disfrases',na=False)), :].Sust_Prueba.replace(r'(?:\s|^)disfrases(?:\s|$)','disfraces ', regex=True)
    df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.strip()
    df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('figurar',na=False)), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('figurar',na=False)), :].Sust_Prueba.replace(r'(?:\s|^)figurar(?:\s|$)','figura ', regex=True)
    df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('librp',na=False)), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('librp',na=False)), :].Sust_Prueba.replace(r'(?:\s|^)librp(?:\s|$)','libro ', regex=True)
    df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('cumpleanos',na=False)), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba.str.contains('cumpleanos',na=False)), :].Sust_Prueba.replace(r'(?:\s|^)cumpleanos(?:\s|$)','cumpleaños ', regex=True)

    df_filtrado= df_filtrado.replace('nan',np.NaN)
    df_filtrado = df_filtrado.replace('Nan',np.NaN)

    df_filtrado.loc[df_filtrado.Lematizado == 'Car','Lematizado'] = 'Cars'

    # como quedan mismas keyword con distinta clasficacion, seleccionamos por ahora la primera
    df_filtrado.drop_duplicates(inplace=True)
    df_filtrado = df_filtrado.groupby('Keyword', as_index=False).nth([0]) 

    df_filtrado.loc[(df_filtrado.Personaje == 'Pantera Negra') & (df_filtrado.Sust_Prueba == 'pantera'),'Sust_Prueba'] = np.NaN
    df_filtrado.loc[df_filtrado.Sust_Prueba == 'paar maravilla',['Lematizado','Sust_Lematizado','Sust_Prueba']] = 'Alicia En El Pais De Las Maravillas',np.NaN,np.NaN
    df_filtrado.loc[df_filtrado.Lematizado.isin(['Lanzar Telaraña Spiderman','Spiderman Lanzar Telaraña']),['Lematizado','Sust_Lematizado','Sust_Prueba']] = 'Spiderman Lanza Telaraña','lanza telaraña','lanza telaraña'
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'lightning mcqueen') & (df_filtrado.Keyword.str.contains(r"\b(shirt|t shirt|t shirts)\b")),['Sust_Lematizado','Sust_Prueba']] = 'Shirt'
    df_filtrado.loc[(df_filtrado.Sust_Prueba.isin(['lightning mcqueen','lightning'])),['Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[(df_filtrado.Keyword.str.contains(r"\b(mcqueen)\b")) & (df_filtrado.Personaje.isin(['Pixar','Relampago Mcqueen', 'Lightnin Mcqueen'])),['Personaje','Personaje2']] = 'Rayo Mcqueen', 'Rayo Mcqueen'
    df_filtrado.loc[(df_filtrado.Keyword.str.contains(r"\b(beauty and the beast)\b")),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','La bella y la bestia','Bella Bestia','Princesas','Princesas','La bella y la bestia'
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'beast') & (df_filtrado.Personaje == 'La Bella Y La Bestia'),['Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[df_filtrado.Sust_Prueba == 'zombie',['Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[(df_filtrado.Sust_Prueba.isin(['niña','nina nina','ana'])) & (df_filtrado.Keyword.str.contains(r"\b(vestido|vestidos)\b")),['Sust_Lematizado','Sust_Prueba']] = 'Disfraz','Disfraz'
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'thano') & (df_filtrado.Keyword == 'thanos') ,['Lematizado','Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'arte') &  (df_filtrado.Keyword.str.contains(r"\b(antiestres)\b")), 'Sust_Lematizado' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba == 'arte') &  (df_filtrado.Keyword.str.contains(r"\b(antiestres)\b")), :].Sust_Lematizado.replace(r'(?:\s|^)arte(?:\s|$)','Arte Antiestres', regex=True)
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'arte') &  (df_filtrado.Keyword.str.contains(r"\b(antiestres)\b")), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba == 'arte') &  (df_filtrado.Keyword.str.contains(r"\b(antiestres)\b")), :].Sust_Prueba.replace(r'(?:\s|^)arte(?:\s|$)','Arte Antiestres', regex=True)
    df_filtrado.loc[df_filtrado.Keyword.str.contains(r"\b(dinoco)\b"),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Dinoco','Dinoco','Pixar','Cars','Cars'
    df_filtrado.loc[df_filtrado.Sust_Prueba.isin(['dinoco','dinoco toy']),['Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[df_filtrado.Keyword == 'blanca nieves',['Lematizado','Sust_Lematizado','Sust_Prueba']] = 'Blancanieves', np.NaN,np.NaN
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'niño') & (df_filtrado['Grupo/Empresa/owner derechos']=='TWDC') & (df_filtrado.Keyword.str.contains(r"\b(cubrebocas|barbijo)\b")),['Sust_Lematizado','Sust_Prueba']] = 'Cubrebocas','Cubrebocas'
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'niño') & (df_filtrado['Grupo/Empresa/owner derechos']=='TWDC') & (df_filtrado.Keyword.str.contains(r"\b(difras)\b")),['Sust_Lematizado','Sust_Prueba']] = 'Disfraz','Disfraz'
    df_filtrado.loc[(df_filtrado.Keyword.isin(['repuestos mahindra scorpio fortaleza','o preco do lp soul soul','pluton','linha princesa','compacto soul vinil','marbel run','marbel 30','maquina de coser portatil magic stitch cordless la tv','magic stitch deluxe','precio por kilo de cuarzo en bruto'])),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido','Sust_Lematizado','Sust_Prueba']] = np.NaN
    df_filtrado.loc[df_filtrado.Keyword.str.contains(r"\b(jumbao)\b"),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido']] = 'Blizzard Entertainment','Jumbao','Jumbao', 'Warcraft','World of Warcraft','World of Warcraft'
    df_filtrado.loc[df_filtrado.Keyword.isin(['alicia en el país de las maravillas',
        'Alicia en el país de las maraviloas', 'alicia país maravillas',
        'alicia país', 'alicia en el país maravillas']),['Lematizado','Sust_Lematizado','Sust_Prueba']] = 'Alicia En El Pais De Las Maravillas',np.NaN,np.NaN
    df_filtrado.loc[df_filtrado.Sust_Prueba.isin(['fiesta','cumpleaños','cumpleanos']),'Sust_Prueba'] = 'Cotillon'
    df_filtrado.loc[df_filtrado['Personaje'] == 'Minnie','Personaje'] = 'Minnie Mouse'
    df_filtrado.loc[df_filtrado.Lematizado == 'Descendiente','Lematizado'] = 'Descendientes'
    df_filtrado.loc[(df_filtrado.Keyword.str.contains(r"\b(vaina vainilla)\b")),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'house') &  (df_filtrado.Keyword.str.contains(r"\b(licuadora)\b")), 'Sust_Prueba' ] = df_filtrado.loc[(df_filtrado.Sust_Prueba == 'house') &  (df_filtrado.Keyword.str.contains(r"\b(licuadora)\b")), :].Sust_Prueba.replace(r'(?:\s|^)house(?:\s|$)','House Licuadora', regex=True)
    df_filtrado.loc[(df_filtrado.Keyword == "pelucia disneys") & ~(df_filtrado.Personaje.isna()),['Lematizado','Sust_Lematizado','Sust_Prueba']] = 'Peliculas Disney','Peliculas','Peliculas'
    df_filtrado.loc[df_filtrado.Sust_Prueba == 'mascar','Sust_Prueba'] = 'mascara'
    df_filtrado.loc[(df_filtrado.Sust_Prueba == 'traje') & ~(df_filtrado.Personaje.isna()),'Sust_Prueba'] = 'Disfraz'
    from nltk.corpus import stopwords
    stop_words = stopwords.words('spanish')[:30] +  ['the','on','of','and']

    def remove_stop_words(data):
        new_text = ""
        words = word_tokenize(str(data))
        for w in words:
            if w not in stop_words and len(w) > 1:
                new_text = new_text + " " + w
                
        return new_text.strip()

    df_filtrado['tiene_subfranquicia'] = np.where(df_filtrado['Sub-Franquicia'].isna(),'NO','SI')
    df_filtrado['tiene_personaje']=np.where(df_filtrado.Personaje.isna(),'NO','SI')
    df_filtrado['tiene_marca']=np.where(df_filtrado.Marca.isna(),'NO','SI')
    df_filtrado['tiene_franquicia']=np.where(df_filtrado.Franquicia.isna(),'NO','SI')
    df_filtrado['Branded'] = np.where((df_filtrado['tiene_marca']=='NO')&(df_filtrado['tiene_subfranquicia']=='NO')&(df_filtrado['tiene_personaje']=='NO'),'Unbranded','Branded')
    df_filtrado['TWDC'] = np.where(df_filtrado['Grupo/Empresa/owner derechos']=='TWDC',1,0)
    df_filtrado['Personaje'] = df_filtrado['Personaje'].str.lower()
    df_filtrado['Personaje2'] = df_filtrado['Personaje'].apply(lambda row: remove_stop_words(row))
    df_filtrado['keyword2'] = df_filtrado['Keyword3'].apply(lambda row: remove_stop_words(row))
    df_filtrado['Personaje'] = df_filtrado['Personaje'].str.title()
    df_filtrado['Personaje2'] = df_filtrado['Personaje2'].str.title()
    df_filtrado['Marca'] = df_filtrado['Marca'].str.title()
    #df_filtrado = df_filtrado_procesado.append(df_filtrado,ignore_index=True)
    df_filtrado = df_filtrado.replace('nan',np.NaN)
    df_filtrado = df_filtrado.replace('Nan',np.NaN)
    #df_filtrado['Lematizado'] = df_filtrado['Lematizado'].str.title()
    #df_filtrado['Sust_Prueba'] = df_filtrado['Sust_Prueba'].str.title()
    df_filtrado['Marca'] = df_filtrado['Marca'].str.title()
    df_filtrado.sort_values(by=['Fecha','Pais','Categoria','Keyword'],inplace=True)



    df.to_csv(PATHS.FINAL_PROCESADO_FILE_CSV, encoding='utf 8-sig', index=False,sep=';')

def main_sustantivos():
    run()


if __name__ == '__main__':
    main_sustantivos()
