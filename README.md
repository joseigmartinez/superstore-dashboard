# 🛒 SuperStore Dashboard

Este proyecto trata sobre la creación de un mini-dashboard luego de realizar una exploración de datos.
El dataset contiene el detalle de las operaciones realizadas por un gigante de los supermercados americanos.

⸻

# 🛠️ Proceso de construcción

1️⃣ Obtención del CSV de una fuente externa (Kaggle)

🔗 Dataset en Kaggle: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final

2️⃣ Almacenamiento en una base de datos SQLite, estructurada y normalizada en tablas.

3️⃣ Consultas SQL para obtener los resultados más importantes.

4️⃣ Visualizaciones de KPIs y métricas clave.

⸻

# 📂 Estructura del proyecto
01_data_uploading.py # Script para crear la base de datos desde el notebook

02_notebook_dashboard.ipynb # Notebook exploratorio para clonar el repositorio, crear la base de datos, explorar los datos y crear el dashboard

README.md # Documentación del proyecto

⸻

# 🚀 Instrucciones de uso

1️⃣ Clonar el repositorio:

!git clone https://github.com/joseigmartinez/superstore-dashboard.git

2️⃣ Ubicarse en la carpeta que contiene el dataset:

%cd superstore-dashboard

3️⃣ Ejecutar el script que crea la base de datos y su estructura:

!python 01_data_loading.py


4️⃣ Abrir el notebook en Google Colab:

-Subir o abrir 02_notebook_dashboard.ipynb

Ejecutar todas las celdas para:

📥 Cargar los datos

🗄️ Crear la base SQLite

📊 Realizar consultas SQL

📈 Generar los gráficos
 
⸻

# 📈 Visualizaciones incluidas:
- KPIS.
  
- Gráfico de línea: Evolución de profit por año.
  
- Gráfico de barras: Ventas por año.

- Gráfico de torta: Porcentaje de órdenes por categoria.

- Gráficos de torta: Órdenes y ganancias por región.

- Gráfico de Ganancias (Profit) por Región.

- Gráfico de tortas: Porcentaje de órdenes por ShipMode (Metodo de envío).

- Gráfico de lineas: Evolución de profit por años.

- MINI-DASHBOARD DE RENDIMIENTO DE SUPERSTORE

⸻

👨‍🏫 Profesor: Juan Carlos Cifuentes Durán

📅 Proyecto académico – Programación Avanzada para Ciencia de Datos, Universidad de la Ciudad de Buenos Aires (2025)
