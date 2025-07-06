from selenium import webdriver
from selenium.webdriver.firefox.options import Options

# Dados
import psycopg2
from sqlalchemy import create_engine

# cloudscraper da bypass no cloudfire (sistema de segurança)
import cloudscraper

from bs4 import BeautifulSoup

import pandas as pd
import os 
import datetime

import unicodedata

from dotenv import load_dotenv
import json

def startExtraction():
    load_dotenv(dotenv_path="../.env", override=True)
    createTablesIfNotExist()
    options = Options()
    options.add_argument('window-size=1200,800')
    driver = webdriver.Firefox(options=options)
    beginExtraction(driver)
    cars = getCars(driver)
    
    if(len(cars) > 0):
        print(cars)
        loadRawJson(cars)
        dfs = transform(cars)
        loadDataFramesToPostgres(dfs)    
    else:
        print("Não foi possível pegar os dados dos carros!")

def beginExtraction(driver):
    url = "https://www.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios?pe=50000"
    driver.get(url)

def getCars(driver):
    foundedCars = []
    page = BeautifulSoup(driver.page_source, 'html.parser')
    cars = page.findAll('section', class_=['olx-adcard', 'olx-adcard__vertical'])
    scraper = cloudscraper.create_scraper()

    for car in cars:
        car_show_url = car.find('a', class_='olx-adcard__link')['href']
        r = scraper.get(car_show_url)
        if r.status_code == 200:
            showCarPage = BeautifulSoup(r.text, 'html.parser')
            carInfos = showCarPage.findAll('div', class_=['ad__sc-2h9gkk-0'])
            if(carInfos):
                carData = {}
                carData['titulo_anuncio'] = showCarPage.find('div', id='description-title').find(class_=['olx-text', 'olx-text--title-medium']).text
                carData['Região'] = car.find('p', class_='olx-adcard__location').text
                carData['Preço'] = car.find('h3', class_='olx-adcard__price').text
                for carInfo in carInfos:
                    infoTitle = carInfo.find('span', class_='olx-text')
                    infoValue = carInfo.find(['a','span'], class_=['olx-link','ad__sc-hj0yqs-0'])                    
                    carInfoTitle = infoTitle.text if infoTitle else None
                    carInfoValue = infoValue.text if infoValue else None
                    
                    carData[carInfoTitle] = carInfoValue
                    
                print(carData)
                foundedCars.append(carData)
        else:
            print(r.status_code)
            
    return foundedCars

def transform(data):    
    df = pd.DataFrame(data)

    # Montadoras
    df_manufactures = df[['Marca']].drop_duplicates().reset_index(drop=True)

    # Região -> Cidade + UF
    if "Região" in df.columns:
        df[['Cidade', 'UF']] = df['Região'].str.split(' - ', expand=True)
        df.drop(columns=['Região'], inplace=True)

    # Estados
    df_ufs = df[['UF']].drop_duplicates().reset_index(drop=True)
    
    # Cidades
    df_cidades = df[['Cidade', 'UF']].drop_duplicates().reset_index(drop=True)

    df_carros = df

    return {
        "df_estado": df_ufs,
        "df_cidades": df_cidades,
        "df_montadoras": df_manufactures,
        "df_carros": df_carros
    }

    
def loadRawJson(data):
    print(data)
    os.makedirs("../data/carros",exist_ok=True)
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_name = f"../data/carros/carros_{now}.json"

    structered_data = {
        "data": data,
        "ingestion_date": now
    }
    
    with open(file_name, "w") as opened_file:
        json.dump(structered_data, opened_file, indent=2)
            
def loadDataFramesToPostgres(dfs):
    db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DATAWAREHOUSE_NAME')}"
    engine = create_engine(db_url)
    conn = engine.connect()
    loadStates(dfs["df_estado"], conn)
    loadManufactures(dfs["df_montadoras"], conn)
    loadCities(dfs["df_cidades"], conn)
    loadCars(dfs["df_carros"], conn)

def loadStates(data, conn):
    for _, row in data.iterrows():
        query = """
            INSERT INTO tb_estado (uf)
            VALUES (%s)
            ON CONFLICT (uf) DO NOTHING
        """
        conn.execute(query, (row["UF"].strip(),))
    
def loadManufactures(data, conn):
    for _, row in data.iterrows():
        query = """
            INSERT INTO tb_montadora (marca)
            VALUES (%s)
            ON CONFLICT (marca) DO NOTHING
        """
        conn.execute(query, (row["Marca"].strip(),))
        
def loadCities(data, conn):
    states = conn.execute("SELECT id, uf FROM tb_estado").fetchall()
    # cria um dicionátio de states onde chave é o estado e o valor é o id {SP : 1,}
    state_dict = {uf:id for id, uf in states}
    
    query = """INSERT INTO tb_cidade (cidade, estado_id)
        VALUES (%s, %s)
        ON CONFLICT (cidade) DO NOTHING
    """

    for _, row in data.iterrows():
        uf = row["UF"].strip()
        state_id = state_dict.get(uf)
        
        if state_dict is not None:
            conn.execute(query, (row["Cidade"], state_id))
        else:
            print(f"estado {uf} não encontrato")
            
def normalize(s):
    return unicodedata.normalize('NFKD', s.strip().lower()).encode('ASCII', 'ignore').decode()
            
def loadCars(data, conn):
    manufactures = conn.execute("SELECT id, marca FROM tb_montadora").fetchall()
    manufactures_dict = {marca:id for id, marca in manufactures}
    cities = conn.execute("SELECT id, cidade FROM tb_cidade").fetchall()
    cities_dict = {normalize(cidade):id for id, cidade in cities}
    
    query = """
        INSERT INTO tb_carro (
            titulo_anuncio,
            categoria,
            modelo,
            tipo_de_veiculo,
            ano,
            quilometragem,
            potencia_do_motor,
            combustivel,
            cambio,
            direcao,
            cor,
            preco,
            cidade_id,
            montadora_id
        ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (titulo_anuncio) DO NOTHING
        """
        
    for _, row in data.iterrows():
        marca = row["Marca"].strip()
        marca_id = manufactures_dict.get(marca)
        cidade = normalize(row["Cidade"].strip())
        city_id = cities_dict.get(cidade)
        conn.execute(query, (
            row["titulo_anuncio"],
            row["Categoria"],
            row["Modelo"],
            row["Tipo de veículo"],
            row["Ano"],
            row["Quilometragem"],
            row["Potência do motor"],
            row["Combustível"],
            row["Câmbio"],
            row["Direção"],
            row["Cor"],
            float(row["Preço"].replace("R$", "").replace(".", "").replace(",", ".").strip()),
            city_id,
            marca_id
        ))
        
        
    
def getPostgresConn():
    load_dotenv(dotenv_path="../.env")  
    print(f'Conectando ao banco {os.getenv("DATAWAREHOUSE_NAME")}')  
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DATAWAREHOUSE_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )      
        return conn 
    except Exception as e:
        print(f"Erro ao conectar com a base de dados: {e}")
    

def createTablesIfNotExist():
    conn = getPostgresConn()  
    with open("../sql/create_tables.sql", "r") as opened_file:
        create_tables_script = opened_file.read()  
    if(conn):
        cursor = conn.cursor()
        cursor.execute(create_tables_script)
        conn.commit()
    else:
        print(f'Não foi possível conectar ao banco de dados {os.getenv("DATAWAREHOUSE_NAME")}')
