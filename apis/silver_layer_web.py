import pandas as pd
import s3fs
import json

# Configuración MinIO
fs = s3fs.S3FileSystem(
    key='minio',
    secret='minio123',
    client_kwargs={'endpoint_url': 'http://localhost:9000'}
)

def load_json_partitioned_by_date(base_path, fs):
    """
    Carga archivos JSON desde una estructura particionada por date=YYYY-MM-DD/
    (solo para SEARCHES)
    """
    files = fs.glob(f"{base_path}/date=*/user*.json")
    dfs = []
    for file in files:
        date_part = file.split("date=")[1].split("/")[0]
        with fs.open(file, 'r') as f:
            json_objects = [json.loads(line) for line in f.readlines()]
            df = pd.DataFrame(json_objects)
            df["date"] = date_part
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_json_flat_folder(prefix_path, fs):
    """
    Carga todos los archivos JSON directamente de una carpeta sin subcarpetas (para REGISTRATIONS)
    """
    files = fs.glob(f"{prefix_path}/user_*.json")
    data = []
    for file in files:
        with fs.open(file, 'r') as f:
            data.append(json.load(f))
    return pd.DataFrame(data) if data else pd.DataFrame()

# Cargar SEARCHES desde estructura particionada por fecha
df_searches = load_json_partitioned_by_date("searches", fs)
df_searches = df_searches.rename(columns={
    "search_date": "timestamp",
    "theme": "search_theme"
})

# Cargar REGISTRATIONS desde ruta plana
df_registrations = load_json_flat_folder("raw/streaming/event_type=registration", fs)
df_registrations = df_registrations.rename(columns={
    "register_date": "timestamp",
    "name": "user_name"
})

# Ver ejemplos
print("SEARCHES:")
print(df_searches.head())

print("\nREGISTRATIONS:")
print(df_registrations.head())

# Guardar como silver en Parquet
df_searches.to_parquet("silver_layer/user_searches.parquet", index=False)
df_registrations.to_parquet("silver_layer/user_registrations.parquet", index=False)
