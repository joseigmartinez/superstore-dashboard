import pandas as pd
import sqlite3
from sqlite3 import Error

# Nombre del archivo CSV y de la base de datos SQLite
CSV_FILE = 'Sample - Superstore.csv' 
DB_FILE = 'superstore.db'

def create_connection(db_file):
    """Crea una conexión a la base de datos SQLite."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        # Necesario para manejar el texto guardado con la coma decimal
        conn.text_factory = str
        return conn
    except Error as e:
        print(f"Error al conectar a SQLite: {e}")
        return None

def import_csv_to_sqlite(csv_file, db_file):
    """
    Lee el CSV, preprocesa los datos y los importa a las 10 tablas definidas.
    """
    print("Iniciando carga de datos con la nueva estructura de tablas...")
    try:
        # 1. Cargar el dataset y limpiar nombres de columnas
        df = pd.read_csv(csv_file, encoding='latin-1',thousands=',')
        df = df.drop(columns=['Row ID'])

        # Limpieza inicial de nombres
        df = df.rename(columns={
            'Customer ID': 'CustomerID', 'Product ID': 'ProductID', 'Order ID': 'OrderID', 
            'Customer Name': 'CustomerName', 'Ship Mode': 'ShipMode', 'Sub-Category': 'Subcategory',
            'Product Name': 'ProductName', 'Order Date': 'OrderDate', 'Ship Date': 'ShipDate',
            'Postal Code': 'PostalCode'
        })

        # 2. Preprocesamiento de Fechas y Años
        def parse_date_and_year(date_series, name):
            datetime_series = pd.to_datetime(date_series, errors='coerce', dayfirst=True, format='mixed')
            # Extraemos el año para el dashboard
            df[f'Year{name}'] = datetime_series.dt.year.astype('Int64')
            # Formateamos la fecha para guardarla como texto limpio
            return datetime_series.dt.strftime('%d-%m-%Y')

        df['OrderDate'] = parse_date_and_year(df['OrderDate'], 'OrderDate')
        df['ShipDate'] = parse_date_and_year(df['ShipDate'], 'ShipDate')
        '''
        # 3. PREPROCESAMIENTO DE VALORES NUMÉRICOS
        for col in ['Sales', 'Discount', 'Profit']:
            # 1. Forzar a numérico primero para evitar ambigüedades en el CSV original
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # 2. Convertir a string con la coma como decimal para guardarlo en la DB
            df[col] = df[col].astype(str).str.replace('.', ',', regex=False)
        '''

        # 4. CREACIÓN DE TABLAS DE DIMENSIÓN Y GENERACIÓN DE IDS

        # 4.1. Region (Dimension: RegionID, Region)
        df_region = df[['Region']].drop_duplicates().sort_values('Region').reset_index(drop=True).reset_index().rename(columns={'index': 'RegionID'})
        df_region['RegionID'] = df_region['RegionID'] + 1
        df = pd.merge(df, df_region, on='Region', how='left')

        # 4.2. City (Dimension: CityID, City)
        df_city = df[['City']].drop_duplicates().sort_values('City').reset_index(drop=True).reset_index().rename(columns={'index': 'CityID'})
        df_city['CityID'] = df_city['CityID'] + 1
        df = pd.merge(df, df_city, on='City', how='left')
        
        # 4.3. Segment (Dimension: SegmentID, Segment)
        df_segment = df[['Segment']].drop_duplicates().sort_values('Segment').reset_index(drop=True).reset_index().rename(columns={'index': 'SegmentID'})
        df_segment['SegmentID'] = df_segment['SegmentID'] + 1
        df = pd.merge(df, df_segment, on='Segment', how='left')
        
        # 4.4. Ship Mode (Dimension: ShipModeID, ShipMode)
        df_ship_mode = df[['ShipMode']].drop_duplicates().sort_values('ShipMode').reset_index(drop=True).reset_index().rename(columns={'index': 'ShipModeID'})
        df_ship_mode['ShipModeID'] = df_ship_mode['ShipModeID'] + 1
        df = pd.merge(df, df_ship_mode, on='ShipMode', how='left')
        
        # 4.5. Category (Dimension: CategoryID, Category)
        df_category = df[['Category']].drop_duplicates().sort_values('Category').reset_index(drop=True).reset_index().rename(columns={'index': 'CategoryID'})
        df_category['CategoryID'] = df_category['CategoryID'] + 1
        df = pd.merge(df, df_category, on='Category', how='left')
        
        # 5. CREACIÓN DE TABLAS FINALES (Dimensiones Compuestas y Hechos)

        # Tabla: Clientes (CustomerID, CustomerName, SegmentID, Country, CityID, State, PostalCode, RegionID)
        df_clientes = df[['CustomerID', 'CustomerName', 'SegmentID', 'Country', 'CityID', 'State', 'PostalCode', 'RegionID']].drop_duplicates(subset=['CustomerID'])

        # Tabla: Productos (ProductID, CategoryID, Subcategory, ProducName)
        df_productos = df[['ProductID', 'CategoryID', 'Subcategory', 'ProductName']].drop_duplicates(subset=['ProductID'])
        
        # Tabla: Ship (OrderID, ShipDate, ShipModeID)
        df_ship = df[['OrderID', 'ShipDate', 'YearShipDate', 'ShipModeID']].drop_duplicates(subset=['OrderID'])
        
        # Tabla: Ordenes (Fact Table) (OrderID, OrderDate, CustomerID, ProductID, Sales, Quantity, Discount, Profit)
        df_ordenes = df[['OrderID', 'OrderDate', 'YearOrderDate', 'CustomerID', 'ProductID', 'Sales', 'Quantity', 'Discount', 'Profit']]


        # 6. Conexión e Importación a SQLite
        conn = create_connection(DB_FILE)
        if conn is not None:
            print("Conexión a la base de datos establecida. Creando y poblando tablas...")
            
            # --- Tablas de Dimensión ---
            df_region.to_sql('Region', conn, if_exists='replace', index=False)
            df_city.to_sql('City', conn, if_exists='replace', index=False)
            df_segment.to_sql('Segment', conn, if_exists='replace', index=False)
            df_ship_mode.to_sql('ShipMode', conn, if_exists='replace', index=False)
            df_category.to_sql('Category', conn, if_exists='replace', index=False)
            
            df_clientes.to_sql('Clientes', conn, if_exists='replace', index=False)
            df_productos.to_sql('Productos', conn, if_exists='replace', index=False)
            
            # --- Tablas de Hechos ---
            df_ship.to_sql('Ship', conn, if_exists='replace', index=False)
            df_ordenes.to_sql('Ordenes', conn, if_exists='replace', index=False)
            
            print(f"\n✅ Carga completada. Base de datos '{DB_FILE}' creada exitosamente con 10 tablas.")
            conn.close()

    except FileNotFoundError:
        print(f"Error: El archivo CSV '{csv_file}' no se encontró. Asegúrate de que esté en el mismo directorio.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        
if __name__ == '__main__':
    # Eliminar el archivo .db anterior si existe para evitar conflictos
    import os
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Archivo {DB_FILE} anterior eliminado.")
        
    import_csv_to_sqlite(CSV_FILE, DB_FILE)