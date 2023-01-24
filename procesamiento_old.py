
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re  

from packages.disney_paths import DisneyPaths
from packages.tools import Tool

DATE = str(dt.today().date()).replace('-', '_')

TOOL = Tool()
PATHS = DisneyPaths(date=DATE)

def run():

    stop_words = stopwords.words('spanish')[:30] +  ['the','on','of','and']

    def remove_stop_words(data):
        new_text = ""
        words = word_tokenize(str(data))
        for w in words:
            if w not in stop_words and len(w) > 1:
                new_text = new_text + " " + w
                
        return new_text.strip()


    def remove_punctuation(data):
        data=str(data)
        data = re.sub(r'[^\w]', ' ', data)
        return data.strip()

    #df_final = pd.read_csv('datos//df_final_procesado_sinjoin.csv',sep=';')

    df_final = TOOL.open_file_xlsx_csv_pkl(PATHS.SAVED_TRENDS)
    df_final = df_final[~df_final.Keyword.isna()]

    df_final = df_final[df_final.Fecha!='Fecha']

    df_final.loc[df_final.Personaje=='It','Personaje'] = np.NaN

    df_final['Fecha']=pd.to_datetime(df_final['Fecha'])
    df_final['Fecha']=df_final['Fecha'].dt.date


    def get_monday(dte):
        return dte - datetime.timedelta(days = dte.weekday())

    df_final['Semana']=df_final['Fecha'].apply(get_monday)


    df_final['Keyword3'] = df_final['Keyword'].apply(lambda row: remove_punctuation(row))


    #df_final=df_final.reset_index()

    df_final=df_final.drop_duplicates()

    #dfclas = pd.read_excel('datos//Personajes.xlsx')
    dfclas = TOOL.open_file_xlsx_csv_pkl(PATHS.PERSONAJES_FILE_XLSX)

    dfclas=dfclas[dfclas['Filtrar']!='Eliminar']
    dfclas=dfclas[dfclas['Filtrar']!='eliminar']

    # saco lo que contiene eliminar
    dfclas = dfclas.apply(lambda x: x.astype(str).str.lower())
    dfclas = dfclas[~dfclas['Filtrar'].str.contains('liminar')]
    dfclas = dfclas[['Grupo/Empresa/owner derechos', 'FRANQUICIA','Sub-Franquicia', 'Contenido','PERSONAJE']]
    # paso todo a minuscula para no tener problemas 
    dfclas = dfclas.drop_duplicates()
    # para ver los duplicados
    dfclas[dfclas.duplicated('PERSONAJE')]
    # armo una lista de Contenido y cuales no estan como personaje
    c = dfclas['PERSONAJE'].unique()
    c1 = [ word for word in dfclas.Contenido.unique() if len(word) >= 3 ]
    dfp = dfclas[~(dfclas.Contenido.isin(c)) & (dfclas.Contenido.isin(c1))]
    dfp = dfp.drop('PERSONAJE',axis=1)
    dfp = dfp.drop_duplicates()
    dfp['PERSONAJE'] = dfp['Contenido']
    #
    ##agrego al dataframe original
    dfclas = dfclas.append(dfp,ignore_index=True)
    #
    dfclas[dfclas.duplicated('PERSONAJE')]
    #repito lo mismo con subfranquicia
    c = dfclas['PERSONAJE'].unique()
    dff = dfclas[~dfclas['Sub-Franquicia'].isin(c)]
    dff = dff.drop(['PERSONAJE','Contenido'],axis=1)
    dff = dff.drop_duplicates()
    dff['PERSONAJE'] = dff['Sub-Franquicia']
    dff['Contenido'] = dff['Sub-Franquicia']
    dff = dff.drop_duplicates()
    #
    ##agrego al dataframe original
    dfclas = dfclas.append(dff,ignore_index=True)
    #
    dfclas[dfclas.duplicated('PERSONAJE')]
    #
    dfclas = dfclas[~((dfclas.FRANQUICIA == 'pixar') & (dfclas.PERSONAJE == 'princesas'))]

    df_personajes = dfclas
    #
    df_final.rename(columns={'Personaje':'PERSONAJE'},inplace=True)


    df_personajes = df_personajes[['Grupo/Empresa/owner derechos','FRANQUICIA','Sub-Franquicia','Contenido','PERSONAJE']]

    df_personajes = df_personajes.drop_duplicates()

    #df_personajes.drop([406],inplace=True)


    sub=df_personajes[['Sub-Franquicia','PERSONAJE']]

    a=sub[sub.duplicated()]
    #
    if a.shape[0]>0:

        raise Exception('HAY PERSONAJES REPETIDOS')
        #Guardar log

    #df_personajes = df_personajes[~((df_personajes.PERSONAJE.isin(['linterna verde','superman'])) & (df_personajes['FRANQUICIA']=='dc comics'))]
    #
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
    #
    df_personajes['PERSONAJE'] = df_personajes['PERSONAJE'].str.strip()

    df_personajes['PERSONAJE']=df_personajes['PERSONAJE'].apply(fix_word)
    df_final['PERSONAJE']=df_final['PERSONAJE'].apply(fix_word)

    df_final['PERSONAJE']=df_final['PERSONAJE'].str.lower()
    df_personajes['PERSONAJE']=df_personajes['PERSONAJE'].str.lower()
    #
    #Testeo
    df=df_final.merge(df_personajes,how='left',on=['PERSONAJE'])
    df['PERSONAJE']=df['PERSONAJE'].str.title()

    #
    df['Grupo/Empresa/owner derechos'] = df['Grupo/Empresa/owner derechos'].replace('\n', '', regex=True)
    df = df.replace('nan',np.NaN)
    df = df.replace('Nan',np.NaN)
    #


    df.rename(columns={'PERSONAJE':'Personaje'},inplace=True)
    df.rename(columns={'FRANQUICIA':'Franquicia'},inplace=True)

    ############# pasar a lower keyword
    #
    df['Keyword3']=df['Keyword3'].apply(fix_word)
    #
    df['Personaje'] = df.Personaje.replace('Starwars','Star Wars', regex=True)
    df['Personaje'] = df.Personaje.replace('Spider Man','Spiderman', regex=True)
    df['Keyword3'] = df.Keyword3.replace(',', ' ', regex=True)
    #lo sgte es dps del merge con marcas
    df['Marca'] = df.Marca.replace('nan', np.NaN, regex=True)
    df['Personaje'] = df.Personaje.replace('Nan', np.NaN, regex=True)
    df['Franquicia'] = df['Franquicia'].str.title()
    df['Sub-Franquicia'] = df['Sub-Franquicia'].str.title()
    df['Contenido'] = df['Contenido'].str.title()
    df['Marca'] = df['Marca'].str.title()
    df['Grupo/Empresa/owner derechos'] = df['Grupo/Empresa/owner derechos'].str.title()
    df['Grupo/Empresa/owner derechos'] = df['Grupo/Empresa/owner derechos'].replace('Twdc','TWDC', regex=True)
    #

    lk = ['alexander mcqueen','sally hansen','Dodge Avenger','loki 29','venzo loki','stranger things',
    'pluton ediciones','instax minni',"bruce lee",'san miguel', 'sao miguel arcanjo',
    'san miguel arcangel', 'fantasias miguel', 'fantasías miguel',
    'arcangel miguel',
    'miguel serrano', 'luis miguel',
    'miguel bose', 'arcangel san miguel','moe','Homer theater','san sebastian',"garrafa térmica aladdin"]
    ## keyword es igual -> FRANQUICIA NULL
    lk2 = ['river',"sauna",'troll']
    #
    #df.loc[df.Keyword.isin(lk2),'Personaje']
    ## junto la lista xq no se puede pasar en contains una lista
    rstr = '|'.join(lk)

    #
    ps = ['ps1','ps2','ps3','ps4','ps5'] # esto habria que pensar que sea con cualquier numero
    ps1 = '|'.join(ps)

    df.loc[df['Keyword3'].str.contains(ps1,na=False),'Marca'] = 'Playstation'
    #df.loc[(df.Keyword3 == 'hammer') & (df.Categoria != 'Salud y Equipamiento Médico'),'Franquicia'] = np.NaN
    #df.loc[(df.Keyword3.str.contains('monster',na=False)) & (df.Categoria=='Alimentos y Bebidas'),'Franquicia'] = 'Monsters'
    df.loc[(df.Keyword3.str.contains(r'(?:\s|^)moto(?:\s|$)',na=False)) & ~(df.Categoria.isin(['Accesorios para Vehículos','Agro','Autos, Motos y Otros','Construcción','Herramientas','Industrias y Oficinas','Inmuebles','Otras categorías', 'Servicios'])),['Marca']] = 'Motorola'
    df.loc[(df.Keyword3.str.contains('russel',na=False)) & (df.Categoria == 'Animales y Mascotas'),['Franquicia']] = np.NaN
    df.loc[(df.Keyword3.str.contains('instax minni',na=False)) ,['Personaje']] = np.NaN
    df.loc[(df.Keyword3.str.contains('aurora',na=False)) & (df.Categoria == 'Electrodomésticos y Aires Ac.'),['Franquicia','Personaje']] = np.NaN

    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)infantil(?:\s|$)',na=False)) & (df.Marca == 'Infanti'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)hugo boss(?:\s|$)',na=False)) & (df.Marca == 'Boss'),'Marca'] = 'Hugo Boss'
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)banda(?:\s|$)',na=False)) & (df.Marca == 'Bandai'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)ambos(?:\s|$)',na=False)) & (df.Marca == 'Cambos'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)brand(?:\s|$)',na=False)) & (df.Marca == 'Brandy'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)light(?:\s|$)',na=False)) & (df.Marca == 'Elight'),'Marca'] = np.NaN
    df.loc[df.Marca == 'Altra','Marca'] = np.where(df.loc[df.Marca == 'Altra']['Keyword3'].str.contains(r'(?:\s|^)altra(?:\s|$)',na=False),'altra',np.NaN)

    df.loc[(df.Keyword3.str.contains(r"\b(airpodss|airpods|apple|ipad|iphone|mac|airpod|macbook)\b",na=False)) & (df.Categoria.isin(['Celulares y Teléfonos','Computación', 'Consolas y Videojuegos','Cámaras y Accesorios',
    'Electrónica, Audio y Video','Instrumentos Musicales', 'Joyas y Relojes'])),'Marca'] = 'Apple'
    df.loc[(df.Keyword3.str.contains(r"\b(galaxy)\b",na=False)) & (df.Categoria.isin(['Celulares y Teléfonos','Computación', 'Consolas y Videojuegos','Cámaras y Accesorios',
    'Electrónica, Audio y Video','Instrumentos Musicales', 'Joyas y Relojes'])),'Marca'] = 'Samsung'
    df.loc[(df['Keyword3'].str.contains(r"\b(chrome cast|chromecast)\b",na=False)),'Marca'] = 'Google'
    df.loc[(df['Keyword3'].str.contains(r"\b(fire tv|fire stick)\b",na=False)) ,'Marca'] = 'Amazon'

    df.loc[(df['Keyword3'].str.contains(r"\b(office)\b",na=False)) & (df.Categoria.isin(['Computación'])),'Marca'] = 'Microsoft'
    df.loc[(df['Keyword3'].str.contains(r"\b(invictus)\b",na=False)),'Marca'] = 'Paco Rabanne'

    df.loc[df['Personaje'] == 'Mickey','Personaje'] = 'Mickey Mouse'
    df.loc[(df['Keyword3'].str.contains(r"\b(Mi Band)\b",na=False)) ,'Marca'] = 'Xiaomi'

    df.loc[df['Personaje'] == 'Minnie','Personaje'] = 'Minnie Mouse'
    #
    df['Marca'] = df.Marca.replace('Hiphone', 'Apple', regex=True)
    df['Marca'] = df.Marca.replace('Bebe', np.NaN, regex=True)
    df['Marca'] = df.Marca.replace('De', np.NaN, regex=True)
    #
    df.loc[(df.Personaje == 'The Office') & (df.Marca == 'Microsoft') & (df.Categoria == 'Computación'),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df.Personaje == 'The Office') & (df.Marca == 'Microsoft') & ~(df.Categoria == 'Computación'),'Marca'] = np.NaN

    df.loc[df.Keyword3.str.contains(r"\b(maui)\b",na=False),'Marca'] = 'Maui And Sons'
    df.loc[df.Personaje.isin(['Alegria','Buho','Carlos','Diamante','Helicoptero','marciano']),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bcry babies\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bmarklin\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bkenner\b",na=False)),'Marca'] = 'Kenner'
    df.loc[df.Personaje == 'Mickey Mouse Mouse',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Mickey Mouse','Mickey Mouse & Friends','Mickey Mouse & Friends','Mickey Mouse & Friends'
    df.loc[(df['Keyword3'].str.contains(r"\btakara tommy\b",na=False)),'Marca'] = 'Takara Tomy'
    df.loc[(df['Keyword3'].str.contains(r"\bbob esponja\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Viacomcbs','Bob Esponja','Bob Esponja','Bob Esponja','Bob Esponja'
    df.loc[df.Personaje.isin(['Gi Joe','Gijoe','G.I.joe']),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Hasbro','Gi Joe','G.I.Joe','G.I.Joe','G.I.Joe'
    df.loc[df.Personaje.isin(['Spider-man','Spiderman','Spider man','Spaiderman','Homem araña','Hombre Araña']),'Personaje'] = 'Spiderman'
    df.loc[df.Personaje == 'Ken','Marca'] = 'Barbie'
    df.loc[df.Keyword3.str.contains(r"\b(league of legends)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Riot Games','League of Legends','League of Legends','League of Legends','League of Legends'
    df.loc[df.Keyword3.str.contains(r"\b(woody)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Woody','Pixar','Toy Story','Toy Story'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)kia soul(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Keyword3.str.contains(r"\b(braven|parlante braven|iron maiden brave new world|gorra atlanta braves|atlanta braves|atlanta brave)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Keyword3.str.contains(r"\b(cassandra clare)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df.Keyword3.str.contains('esmeralda en bruto',na=False)) ,['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df.Keyword3.str.contains(r"\b(psvita|play|pleytellion|juegos play|juegos de play)\b",na=False)) & (df.Marca.isna()) & ~(df.Keyword.str.contains(r"\b(play movil)\b")),'Marca'] = 'Playstation'
    df.loc[(df.Keyword3.str.contains(r"\b(bella)\b",na=False)) & (df.Franquicia =='Disney'),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Bella','Princesas','Princesas','La Bella Y La Bestia'
    df.loc[df.Personaje == 'Squidgame','Personaje'] = 'Squid Game'
    df.loc[(df.Keyword3.str.contains(r"\b(dualshock)\b",na=False)) & (df.Marca.isna()),'Marca'] = 'Playstation'
    #####
    df.loc[df.Keyword3.str.contains(r"\b(cerveza duff|homer theater|homer design|moe pts)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Keyword3.str.contains(r"\b(avatar aang)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Personaje2','Franquicia','Sub-Franquicia','Contenido']] = 'Nickelodeon','Avatar Aang','Avatar Aang','Avatar La Leyenda de Aang','Avatar La Leyenda de Aang','Avatar La Leyenda de Aang'  
    df.loc[(df.Keyword3.str.contains('caballeros del zodiaco',na=False)) & (df.Marca=='Zodiac'),'Marca'] = np.NaN
    df.loc[(df.Keyword3.str.contains('caballeros del zodiaco',na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Toei Animation','Caballeros del Zodiaco','Caballeros del Zodiaco','Caballeros del Zodiaco','Caballeros del Zodiaco'
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)homem de ferro(?:\s|$)',na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Homem De Ferro','Marvel','Avengers','Avengers'
    df.loc[(df.Marca == 'Blade') & ~(df.Categoria.isin(['Camaras y Accesorios','Celulares y Telefonos','Consolas y Videojuegos'])),'Marca'] = np.NaN
    df.loc[df.Personaje.isin(['Barbies','Barbi','Barbi E','Barby']),'Personaje'] = 'Barbie'
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)carro(?:\s|$)',na=False)) & (df.Marca == 'Carrom'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\b(lego|legos)\b",na=False)) ,'Marca'] = 'Lego'
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)casino(?:\s|$)',na=False)) & (df.Marca == 'Casio'),'Marca'] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r'(?:\s|^)formula 1(?:\s|$)',na=False)) & (df.Marca == 'Formulab'),'Marca'] = np.NaN
    df.loc[df.Personaje.isin(['Rick Morty','Rick Y Morty']),'Personaje'] = 'Rick And Morty'
    df.loc[df.Personaje == 'Harley Queen','Personaje'] = 'Harley Quinn'
    df.loc[(df.Keyword3.str.contains(r"\b(queen)\b",na=False)) & (df.Categoria.isin([
        'Instrumentos Musicales', 'Libros, Revistas y Comics',
        'Música, Películas y Series'])),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'EMI','Queen','Queen','Queen','Queen'
    df.loc[(df['Keyword3'].str.contains(r"\bthe walking dead\b",na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','The Walking Dead','20/21Cf','The Walking Dead','The Walking Dead'
    df.loc[(df['Keyword3'].str.contains(r"\bmcfarlane\b",na=False)),'Marca'] = 'Mcfarlane'
    df.loc[(df['Keyword3'].str.contains(r"\bmasters of the universe\b",na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Mattel','masters of the universe','Mattel','He-Man','He-Man'
    df.loc[(df['Keyword3'].str.contains(r"\bmy little pony\b",na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Entertainment One','My Little Pony','My Little Pony','My Little Pony','My Little Pony'
    df.loc[(df['Keyword3'].str.contains(r"\bmy hero academia\b",na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Funimation','My hero Academia','My hero Academia','My hero Academia','My hero Academia'
    df.loc[(df['Keyword'].str.contains(r"\btakara tomy\b",na=False)),'Marca'] = 'Takara Tomy'
    df.loc[df.Marca == 'Rusty',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bps\b",na=False)),'Marca'] = 'Playstation'
    df.loc[df.Personaje== 'Vision',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df.Personaje == 'Neera') & (df['Keyword3'].str.contains(r'(?:\s|^)nevera(?:\s|$)',na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains('ben 10',na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Cartoon Network','Ben 10','Ben 10','Ben 10','Ben 10'
    df.loc[(df['Keyword3']=='neera'),['Personaje']]=np.NaN
    df.loc[(df.Contenido == 'Winnie The Pooh') & (df.Keyword3.str.contains('conejo',na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Personaje== 'Dinosaurios',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Personaje== 'Vision',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN 
    df.loc[df.Keyword3 == 'el señor anillos',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Middle-Earth Enterprises', 'El Señor De Los Anillos','El Señor De Los Anillos','El Señor De Los Anillos','El Señor De Los Anillos'
    #df.loc[(df['Keyword'].str.contains(r"\bps\b",na=False))]=np.NaN
    #####
    df.loc[(df['Keyword3'].str.contains('ben 10',na=False)),
    ['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Cartoon Network','Ben 10','Ben 10','Ben 10','Ben 10'
    ###########
    palabras_filtradas=['sexual','pene','vagina','vibrador','bondage','masaje','masajes','leche','peniana','penianas','preservativo','preservativos','masturbador','masturbado','encuaradas','lubricante','dildo','bdsm','consolador','viagra'
                        ,'sexshop','anal','muñeca inflable','perverso','fuck machine','sado','melzinho','ar nes','arnes',
                        'vibradora','mordaza','sadomasoquismo','eyaculacion','vibrator','esposas','sex','penetracion',
                        'clitoris','prostata','penis','dilatador','tanga','tangas','muñeca inflable','hentai','enema','sexy','tesao',
                        'sexualidad','arnés','clítoris','erótico','sexyshop','powersex','masturbador','castidad','retardante','masturbacion','lenceria','travesti',
                        'porno','pornografía','peniano','pezon','vigor','muñeca sexuale','plug','maconha','anillo erotico'
                        ,'gays','anillo erotico','orgasmo','erección','consolos','agrandador miembro','erotico','gay','cock ring','erotica','masturvador','pené','suspensorios']
    #
    df=df[~df.Keyword3.str.contains('|'.join(palabras_filtradas),na=False)]
    ##########################
    #
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)rapido y furioso(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Comcast - Universal Studios','Rapido y Furioso','Rapido y Furioso','Rapido y Furioso','Rapido y Furioso'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)airo man(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Iron Man','Marvel','Avengers','Avengers'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)angry birds(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Rovio entertainment','Angry Birds','Angry Birds','Angry Birds','Angry Birds'
    df.loc[df.Keyword3.str.contains(r"\b(among|amogus|amung us)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'American Game Studio Innersloth','Among Us','Among Us','Among Us','Among Us'
    df.loc[df.Personaje.isin(['Ataque Titanes','Attack Titan']),'Personaje'] = 'Attack On Titan'
    df.loc[df.Personaje.isin(['Hotwheels','Hotwheel','Hotweels','Hot Wheels','Hot Wels','hotwwehls']),'Personaje'] = 'Hot Wheels'
    df.loc[df.Keyword3.str.contains(r"\b(back to future)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Comcast - Universal Studios','Back To The Future','Back To The Future','Back To The Future','Back To The Future'
    df.loc[df.Personaje.isin(['BayBlade','Beyblade','Bey Blade']),'Personaje'] = 'Beyblade'
    #
    df.loc[df.Keyword3.str.contains(r"\b(boku no hero)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Funimation','My Hero Academia','My Hero Academia','My Hero Academia','My Hero Academia'
    df.loc[df.Keyword3.str.contains(r"\b(lol)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'LOL Surprise Club','LOL Surprise Club','LOL Surprise Club','LOL Surprise Club','LOL Surprise Club'
    df.loc[df.Personaje.isin(['Como Entrenar Dragon']),'Personaje'] = 'Como Entrenar A Tu Dragon'
    df.loc[df.Personaje.isin(['Dead Note']),'Personaje'] = 'Death Note'
    df.loc[df.Personaje.isin(['Blancanieves y los 7 enanitos','Blanca Nieves']),'Personaje'] = 'Blancanieves'
    df.loc[df.Personaje.str.contains(r"\b('dc collectibles'|'dc multiverse'|'dc universe')\b",na=False),'Personaje'] = 'DC Comics'
    df.loc[df.Keyword3.str.contains(r"\b('granja de zenon')\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Leader Music','El Reino Infantil','La Granja De Zenon','La Granja De Zenon','La Granja De Zenon'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)demon slayer(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Funimation','Demon Slayer','Demon Slayer','Demon Slayer','Demon Slayer'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)jurassic world(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Comcast - Universal Studios','Jurassic World','Jurassic Park','Jurassic World','Jurassic World'
    df.loc[df.Keyword3.str.contains(r"\b('star wars')\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Star Wars','Starwars','Starwars','Starwars'
    df.loc[df.Personaje.isin(['Star Wars','Starwars']),'Personaje'] = 'Starwars'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)ever after high(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Mattel','Ever After High','Ever After High','Ever After High','Ever After High'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)mandalorian(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Mandalorian','Starwars','The Mandalorian','The Mandalorian'
    df.loc[(df['Keyword3'].str.contains(r"\bmaster of the universe\b",na=False)) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Mattel','masters of the universe','Mattel','He-Man','He-Man'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)la bella y la bestia(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','La Bella Y La Bestia','Princesas','Princesas','La Bella Y La Bestia'

    df = df[df.Keyword3.notnull()]

    df.loc[df.Personaje == 'Captain America','Personaje'] = 'Capitan America'
    df.loc[df.Personaje == 'IronMan','Personaje'] = 'Iron Man'
    df.loc[df.Personaje.isin(['Spider-man','Spider-Man','Spiderman','Spider Man','Spaiderman','Homem Araña','Hombre Araña','Homem Aranha']),'Personaje'] = 'Spiderman'
    df.loc[df.Personaje.isin(['Star Wars','Starwars']),'Personaje'] = 'StarWars'
    df.loc[(df.Keyword3.str.contains(r"\b(psvita|play|pleytellion|juego play|juegos play)\b")) & (df.Marca.isna()) & ~(df.Keyword.str.contains(r"\b(play movil)\b")),'Marca'] = 'Playstation'
    df.loc[(df.Keyword3.str.contains(r"\b(gameboy|gamecube|wii|nintendo|ds|switch|amiibo|game cube|snes|game boy|game watch)\b")), 'Marca'] = 'Nintendo'


    df.loc[df.Keyword3.str.contains(r"\b(cavaleiros do zodiaco)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Toei Animation','Cavaleiros Do Zodiaco','Caballeros del Zodiaco','Caballeros del Zodiaco','Caballeros del Zodiaco'
    df.loc[df.Keyword3.str.contains(r"\b(como entrenar dragon)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'DreamWorks Animation','Como Entrenar Dragon','Cómo Entrenar A Tu Dragón','Cómo Entrenar A Tu Dragón','Cómo Entrenar A Tu Dragón'
    df.loc[df.Keyword3.str.contains(r"\b(kakashi)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Pierrot company','Kakashi','Naruto','Naruto','Naruto'
    df.loc[df.Keyword3.str.contains(r"\b(cuphead)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'StudioMDHR','Cuphead','Cuphead','Cuphead','Cuphead'
    df.loc[df.Keyword3.str.contains(r"\b(death note|dead note)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Viz Media','Death Note','Death Note','Death Note','Death Note'
    df.loc[df.Keyword3.str.contains(r"\b(dc collectibles|dc multiverse|dc universe)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'WarnerMedia','DC Comics','DC Comics','DC Comics','DC Comics'
    df.loc[df.Keyword3.str.contains(r"\b(deku)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Funimation','Deku','My Hero Academia','My Hero Academia','My Hero Academia'
    df.loc[df.Keyword3.str.contains(r"\b(delorean)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Comcast - Universal Studios','Delorean','Back To The Future','Back To The Future','Back To The Future'
    df.loc[df.Keyword3.str.contains(r"\b(esferas del dragon|esferas dragon|esferas del dragón)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Entertainment One','Esferas Del Dragon','Dragon Ball','Dragon Ball','Dragon Ball'
    df.loc[df.Keyword3.str.contains(r"\b(espada laser jedi)\b"),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Espada Laser Jedi','StarWars','StarWars','StarWars'
    df.loc[(df.Personaje == 'flash') & (df.Keyword3 != 'flash dc'),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[df.Keyword3.str.contains(r"\b(princesa peach|peach princesa)\b") ,['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Nintendo','Princesa Peach','Super Mario Bros',	'Mario Bross',	'Mario Bross'	

    df.loc[df.Personaje.isin(['Alegria','Buho','Carlos','Diamante','Helicoptero','marciano']),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bcry babies\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bmarklin\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bkenner\b",na=False)),'Marca'] = 'Kenner'

    df.loc[df.Personaje == 'Mickey Mouse Mouse',['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Mickey Mouse','Mickey Mouse & Friends','Mickey Mouse & Friends','Mickey Mouse & Friends'
    #
    df.loc[df.Personaje == 'Principito','Personaje'] = 'El Principito'
    df.loc[(df['Keyword3'].str.contains(r"\bnetflix\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Netflix Studios','Netflix','Netflix','Netflix','Netflix'
    df.loc[(df['Keyword3'].str.contains(r"\bsquier\b",na=False)),'Marca'] = 'Squier'
    df.loc[(df['Keyword3'].str.contains(r"\bprs\b",na=False)),'Marca'] = 'Prs'
    df.loc[(df['Keyword3'].str.contains(r"\bhohner\b",na=False)),'Marca'] = 'Hohner'
    df.loc[(df['Keyword3'].str.contains(r"\bmi box\b",na=False)),'Marca'] = 'Xiaomi'
    df.loc[(df['Keyword3'].str.contains(r"\bgtx 1080\b",na=False)),['Marca']] = 'Geforece'
    df.loc[(df['Keyword3'].str.contains(r"\b(red label|blue label)\b",na=False)),'Marca'] = 'Johnnie Walker'
    df.loc[(df['Keyword3'].str.contains(r"\bumbrella academy\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Netflix Studios','Umbrella Academy','Umbrella Academy','Umbrella Academy','Umbrella Academy'
    df.loc[(df['Keyword3'].str.contains(r"\blast of us\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Sony','Last Of Us','Last Of Us','Last Of Us','Last Of Us'
    df.loc[(df['Keyword3'].str.contains(r"\bthermomix\b",na=False)),'Marca'] = 'Vorwerk'
    df.loc[(df['Keyword3'].str.contains(r"\bGtx 1070|gtx 970\b",na=False)),'Marca'] = 'Geforce'
    df.loc[(df['Keyword3'].str.contains(r"\bg shock\b",na=False)),['Marca']] = 'Casio'
    df.loc[(df['Keyword3'].str.contains(r"\bpantera rosa\b",na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'La Pantera Rosa', 'La Pantera Rosa', 'La Pantera Rosa','La Pantera Rosa', 'La Pantera Rosa'
    df.loc[(df['Keyword3'].str.contains(r"\bamarok\b",na=False)),['Marca']] = 'Volkswagen'
    df.loc[(df['Keyword3'].str.contains('ben 10',na=False)),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Cartoon Network','Ben 10','Ben 10','Ben 10','Ben 10'
    df.loc[df.Personaje.isin(['Blancanieves y los 7 enanitos','Blanca Nieves','Snowhite','Snow White']),'Personaje'] = 'Blancanieves'
    df.loc[df.Personaje.isin(['Star Wars','Starwars']),'Personaje'] = 'StarWars'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)la bella y la bestia(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','La Bella Y La Bestia','Princesas','Princesas','La Bella Y La Bestia'
    df.loc[df.Keyword3.str.contains(r'(?:\s|^)rey leon(?:\s|$)',na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','El Rey Leon','Disney','Disney','El Rey Leon'
    df.loc[(df.Personaje == 'Neera') & (df['Keyword3'].str.contains(r"\b(nevera|NEVERA)\b")),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df.Marca == 'Libero') & (df.Keyword3.str.contains(r'(?:\s|^)libro(?:\s|$)')),'Marca'] = np.NaN
    df.loc[(df.Marca == 'Caden') & (df.Keyword3.str.contains(r'(?:\s|^)cadena(?:\s|$)')),'Marca'] = np.NaN
    df.loc[df.Keyword3.str.contains(r"\b(walking dead)\b",na=False),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] =  'TWDC', 'The Walking Dead','20/21Cf','The Walking Dead','The Walking Dead'
    df.loc[(df['Keyword3'].str.contains(r"\bel extraño mundo de jack\b")),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','El Extraño Mundo De Jack','Disney','Disney','The Nightmare Before Christmas'
    df.loc[(df['Personaje'] == 'Queen') & (df.Categoria == 'Hogar, Muebles y Jardín'),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = np.NaN
    df.loc[(df['Keyword3'].str.contains(r"\bmiles morales\b")),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Sony','Miles Morales','Marvel','Spiderman','Spiderman'
    df.loc[df.Personaje.isin(['Ironman','Iron Man','Homem De Ferro']),'Personaje'] = 'Iron Man'
    df.loc[(df['Keyword3'].str.contains(r"\bariel\b")) & (df.Personaje == 'Disney'),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','La Sirenita','Princesas',	'Princesas','La Sirenita'
    df.loc[(df['Keyword3'].str.contains(r"\bcall of duty\b")) & (df.Personaje.isna()),['Grupo/Empresa/owner derechos','Personaje','Franquicia','Sub-Franquicia','Contenido']] = 'Activision','Call of Duty'	,'Call of Duty',	'Call of Duty',	'Call of Duty'
    df.loc[df.Personaje.isin(['Vingadores','Os Vingadores']),'Personaje'] = 'Avengers'
    df.loc[df.Personaje.isin(['Viuda Negra','Viúva Negra']),'Personaje'] = 'Black Widow'
    df.loc[df.Personaje.isin(['Sirenita','A Pequena Sereia']),'Personaje'] = 'La Sirenita'
    df.loc[df.Personaje.isin(['Simpson','Simpsons']),'Personaje'] = 'Los Simpsons'
    df.loc[df.Personaje.isin(['Jasmin']),'Personaje'] = 'Jazmin'
    df.loc[df.Personaje.isin(['Bella']),['Grupo/Empresa/owner derechos','Franquicia','Sub-Franquicia','Contenido']] = 'TWDC','Princesas','Princesas','La Bella Y La Bestia'
    df.loc[df.Personaje.isin(['Stranger Thing']),'Personaje'] = 'Stranger Things'

    df.rename(inplace=True,columns={'FRANQUICIA':'Franquicia','PERSONAJE':'Personaje'})
    df = df.drop_duplicates()
    df = df[df.Keyword.notnull()]

    #df.to_csv('datos//df_final_procesado_sinprod.csv',index=False,encoding='utf-8 sig',sep=';')
    #pd.to_pickle(df, PATHS.FINAL_PROCESADO_SIN_PROD_PKL)
    df.to_csv(PATHS.FINAL_PROCESADO_SIN_PROD_CSV, index=False,encoding='utf-8 sig', sep=";")

    #df = TOOL.open_file_xlsx_csv_pkl(PATHS.FINAL_PROCESADO_FILE_CSV)

def main_processor():
    run()

if __name__ == '__main__':
    main_processor()
