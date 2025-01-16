# Proyecto: Análisis y Transformación de Reportes Financieros de la SEC

## Descripción
Este proyecto automatiza el proceso de extracción y transformación de datos financieros de los reportes 10-K de la SEC a un formato estructurado. Realiza las siguientes tareas:

1. Descarga archivos HTML de reportes financieros.
2. Analiza las tablas clave:
   - Estados Consolidados de Operaciones
   - Balances Generales Consolidados
   - Estados Consolidados de Flujos de Efectivo
3. Guarda los datos analizados en archivos Parquet dentro de la carpeta `silver/`.
4. Carga los datos Parquet en bases de datos SQLite almacenadas en la carpeta `gold/`.
5. Valida la creación de tablas en las bases de datos SQLite.

El resultado es un conjunto de datos estructurados y validados listos para análisis posterior.

---

## Instalación

### Requisitos Previos
- Python 3.8 o superior
- Entorno virtual (recomendado)
- Conexión a internet para descargar los reportes 10-K de la SEC

### Instalación de Dependencias
1. Clona el repositorio:
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```
2. Crea y activa un entorno virtual:
```bash
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```
3. Instala las bibliotecas necesarias:
```bash
Copiar código
pip install -r requirements.txt
```
### requirements.txt
El proyecto utiliza las siguientes dependencias:

* pandas: Para operaciones con DataFrames y manejo de archivos.
* beautifulsoup4: Para analizar el contenido HTML.
* requests: Para descargar los reportes 10-K de la página de la SEC.
* pyarrow: Para guardar datos en formato Parquet.
* sqlite3: Para manejar bases de datos SQLite.

## Uso
Paso 1: Ejecutar el Script
Ejecuta el script con el siguiente comando:

```bash
python etl_pipeline.py
```
## Paso 2: Archivos Generados

1. El script genera los siguientes resultados:

   * Carpeta Bronze:

Contiene los archivos HTML sin procesar descargados desde la página de la SEC.

2. Carpeta Silver:

   * Contiene las tablas financieras analizadas y guardadas en formato Parquet, con nombres como:

      * consolidated_statements_of_operations_10k_2020.parquet

      * consolidated_balance_sheets_10k_2021.parquet

3. Carpeta Gold:

   * Bases de datos SQLite con tablas creadas a partir de los archivos Parquet procesados.

## Paso 3: Validar Resultados

Los registros de validación se muestran en la consola para confirmar la creación exitosa de las tablas SQLite y los archivos Parquet.

## Flujo de Trabajo del Script

1. Descarga de Archivos HTML:

   * El script descarga los reportes 10-K desde las URLs de la SEC y los guarda en la carpeta bronze/.

2. Análisis de Tablas Financieras:

   * Extrae tablas basadas en encabezados y estilos predefinidos.

   * Convierte las tablas en DataFrames de Pandas.

   * Guarda las tablas en archivos Parquet dentro de la carpeta silver/.

3. Transformación y Carga:

   * Carga los archivos Parquet en bases de datos SQLite dentro de la carpeta gold/.

4. Validación de Datos:

   * Verifica que las tablas SQLite se hayan creado correctamente.

Estructura de Archivos
```graphql
<directorio_del_repositorio>/
├── bronze/               # Archivos HTML sin procesar
├── silver/               # Datos financieros analizados en formato Parquet
├── gold/                 # Bases de datos SQLite
├── requirements.txt      # Dependencias de Python
├── etl_pipeline.py # Script principal
└── README.md             # Documentación del proyecto
```