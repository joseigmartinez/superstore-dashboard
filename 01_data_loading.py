import pandas as pd
import sqlite3
from sqlite3 import Error

# Nombre del archivo CSV y de la base de datos SQLite
CSV_FILE = 'Sample - Superstore.csv'
DB_FILE = 'superstore.db'

def create_connection(db_file):
    """Crea una conexión a la base de datos SQLite especificada por db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.text_factory = str
        return conn
    except Error as e:
        print(f"Error al conectar a SQLite: {e}")
        return None

def import_csv_to_sqlite(csv_file, db_file):
    """
    Lee el CSV, preprocesa los datos y los importa a las tablas normalizadas
    en la base de datos SQLite.
    """
    try:
        # 1. Cargar el dataset, con la codificación corregida y excluyendo 'Row ID'
        df = pd.read_csv(csv_file, encoding='latin-1',thousands=',')
        df = df.drop(columns=['Row ID'])

        # Renombrar IDs para evitar errores de espacios en blanco
        df = df.rename(columns={'Customer ID': 'CustomerID', 'Product ID': 'ProductID', 'Order ID': 'OrderID'})

        # 2. Preprocesamiento de Fechas y Años
        def parse_date_and_year(date_series):
            datetime_series = pd.to_datetime(date_series, errors='coerce', dayfirst=True, format='mixed')
            year_series = datetime_series.dt.year.astype('Int64')
            formatted_date_series = datetime_series.dt.strftime('%d-%m-%Y')
            return formatted_date_series, year_series

        df['Order Date'], df['YearOrderDate'] = parse_date_and_year(df['Order Date'])
        df['Ship Date'], df['YearShipDate'] = parse_date_and_year(df['Ship Date'])
        '''
        # 3. Preprocesamiento de Valores Numéricos (reemplazo '.' por ',')
        for col in ['Sales', 'Discount', 'Profit']:
            #df[col] = df[col].astype(str).str.replace('.', ',', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')'''      

        # 4. Creación de Tablas de Dimensión y Normalización

        # 4.1. Region
        df_region = df[['Region']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'RegionID'})
        df_region['RegionID'] = df_region['RegionID'] + 1
        df = pd.merge(df, df_region, on='Region', how='left')

        # 4.2. City
        # Tabla: City (CityID, City)
        df_city = df[['City']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'CityID'})
        df_city['CityID'] = df_city['CityID'] + 1
        df = pd.merge(df, df_city, on='City', how='left')
        
        # Tabla: State (CityID, State, PostalCode, RegionID)
        # Usamos CityID como clave foránea a City
        df_state = df[['CityID', 'State', 'Postal Code', 'RegionID']].drop_duplicates().rename(columns={'Postal Code': 'PostalCode'})

        # 4.3. Segment
        df_segment = df[['Segment']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'SegmentID'})
        df_segment['SegmentID'] = df_segment['SegmentID'] + 1
        df = pd.merge(df, df_segment, on='Segment', how='left')
        
        # 4.4. Ship Mode
        df_ship_mode = df[['Ship Mode']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'ShipModeID'})
        df_ship_mode['ShipModeID'] = df_ship_mode['ShipModeID'] + 1
        df = pd.merge(df, df_ship_mode, on='Ship Mode', how='left')
        
        # 4.5. Category
        df_category = df[['Category']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index': 'CategoryID'})
        df_category['CategoryID'] = df_category['CategoryID'] + 1
        df = pd.merge(df, df_category, on='Category', how='left')
        
        # 4.6. Subcategory
        df_sub_category = df[['CategoryID', 'Sub-Category', 'Product Name']].drop_duplicates().rename(columns={'Sub-Category': 'Subcategory', 'Product Name': 'ProductName'})
        
        # 4.7. Clientes (Recuperamos los datos para la dimensión)
        df_clientes = df[['CustomerID', 'Customer Name', 'SegmentID', 'CityID']].drop_duplicates(subset=['CustomerID']).rename(columns={'Customer Name': 'CustomerName'})
        df_clientes['Country'] = 'United States'
        
        # 4.8. Productos (ProductID, CategoryID)
        df_productos = df[['ProductID', 'CategoryID']].drop_duplicates()
        
        # --- Eliminación de columnas intermedias para el DF de hechos ---
        cols_to_drop = ['Ship Mode', 'Segment', 'Country', 'City', 'State', 'Postal Code', 'Region', 'Category', 'Sub-Category', 'Customer Name']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns if col not in ['CustomerID', 'ProductID', 'OrderID', 'CityID']])


        # 5. Conexión e Importación a SQLite
        conn = create_connection(db_file)
        if conn is not None:
            print("Conexión a la base de datos establecida. Creando tablas...")
            
            # --- Tablas de Dimensión ---
            
            # Tabla: Region
            df_region.to_sql('Region', conn, if_exists='replace', index=False)
            print("Tabla 'Region' creada.")
            
            # Tabla: City
            df_city.to_sql('City', conn, if_exists='replace', index=False)
            print("Tabla 'City' creada.")
            
            # Tabla: State
            df_state.to_sql('State', conn, if_exists='replace', index=False)
            print("Tabla 'State' creada.")
            
            # Tabla: Ship Mode
            df_ship_mode.rename(columns={'Ship Mode': 'ShipMode'}).to_sql('ShipMode', conn, if_exists='replace', index=False)
            print("Tabla 'ShipMode' creada.")
            
            # Tabla: Segment
            df_segment.to_sql('Segment', conn, if_exists='replace', index=False)
            print("Tabla 'Segment' creada.")
            
            # Tablas de Producto
            df_category.to_sql('Category', conn, if_exists='replace', index=False)
            print("Tabla 'Category' creada.")
            
            df_sub_category.to_sql('Subcategory', conn, if_exists='replace', index=False)
            print("Tabla 'Subcategory' creada.")

            df_productos.to_sql('Productos', conn, if_exists='replace', index=False)
            print("Tabla 'Productos' creada.")
            
            # Tabla: Clientes
            df_clientes[['CustomerID', 'CustomerName', 'SegmentID', 'Country', 'CityID']].to_sql('Clientes', conn, if_exists='replace', index=False)
            print("Tabla 'Clientes' creada.")
            
            # --- Tablas de Hechos ---
            
            # Tabla: Ship
            df_ship = df[['OrderID', 'Ship Date', 'YearShipDate', 'ShipModeID']].drop_duplicates().rename(columns={'Ship Date': 'ShipDate'})
            df_ship.to_sql('Ship', conn, if_exists='replace', index=False)
            print("Tabla 'Ship' creada.")
            
            # Tabla: Ordenes
            df_ordenes = df[['OrderID', 'Order Date', 'YearOrderDate', 'CustomerID', 'ProductID', 'Sales', 'Quantity', 'Discount', 'Profit']].rename(columns={'Order Date': 'OrderDate'})
            df_ordenes.to_sql('Ordenes', conn, if_exists='replace', index=False)
            print("Tabla 'Ordenes' creada.")
            
            print("\n✅ Importación completada. Base de datos 'superstore.db' lista.")
            conn.close()

    except FileNotFoundError:
        print(f"Error: El archivo CSV '{csv_file}' no se encontró.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

# Ejecutar la función
import_csv_to_sqlite(CSV_FILE, DB_FILE)