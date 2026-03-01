"""
Parte 1 — Preparación del Data Warehouse
=========================================
Ejecutar con: python parte1_data_warehouse.py

Pasos:
  1.1 Crear datasets: dw_staging, dw_core, dw_serving
  1.2 Cargar ventas_2024.csv en dw_staging.stg_ventas (particionada por fecha, clusterizada por region)
  1.3 Crear tabla fact_ventas en dw_core con campo calculado 'total'
  1.4 Crear vista v_resumen_ventas en dw_serving
"""

from google.cloud import bigquery

PROYECTO = 'clau-personal'
REGION = 'us-central1'

client = bigquery.Client(project=PROYECTO)

# ============================================================
# 1.1 Creación de datasets
# ============================================================
print("=" * 60)
print("1.1 — Creando datasets...")
print("=" * 60)

datasets = ['dw_staging', 'dw_core', 'dw_serving']

for ds_name in datasets:
    ds_id = f"{PROYECTO}.{ds_name}"
    dataset = bigquery.Dataset(ds_id)
    dataset.location = REGION
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"  Dataset '{ds_name}' creado/verificado en {REGION}")

# ============================================================
# 1.2 Carga del archivo CSV en dw_staging.stg_ventas
# ============================================================
print("\n" + "=" * 60)
print("1.2 — Cargando ventas_2024.csv en dw_staging.stg_ventas...")
print("=" * 60)

table_id = f"{PROYECTO}.dw_staging.stg_ventas"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="fecha",
    ),
    clustering_fields=["region"],
)

with open("ventas_2024.csv", "rb") as f:
    load_job = client.load_table_from_file(f, table_id, job_config=job_config)

load_job.result()  # Esperar a que termine

table = client.get_table(table_id)
print(f"  Tabla stg_ventas cargada: {table.num_rows} filas")
print(f"  Particionada por: {table.time_partitioning.field}")
print(f"  Clusterizada por: {table.clustering_fields}")

# ============================================================
# 1.3 Creación de fact_ventas en dw_core
# ============================================================
print("\n" + "=" * 60)
print("1.3 — Creando dw_core.fact_ventas...")
print("=" * 60)

query_fact = f"""
CREATE OR REPLACE TABLE `{PROYECTO}.dw_core.fact_ventas`
PARTITION BY fecha
CLUSTER BY region
AS
SELECT
    id_venta,
    fecha,
    region,
    categoria,
    producto,
    cantidad,
    precio_unitario,
    descuento_pct,
    vendedor,
    ROUND(cantidad * precio_unitario * (1 - descuento_pct / 100), 2) AS total,
    CURRENT_TIMESTAMP() AS _procesado_at
FROM `{PROYECTO}.dw_staging.stg_ventas`
"""

client.query(query_fact).result()
print("  Tabla fact_ventas creada exitosamente.")

# Consulta de evidencia 1.3
print("\n  --- Evidencia 1.3 ---")
query_evidencia_13 = f"""
SELECT region, COUNT(*) AS ventas, ROUND(SUM(total),2) AS total_ventas
FROM `{PROYECTO}.dw_core.fact_ventas`
GROUP BY region
ORDER BY region
"""
df = client.query(query_evidencia_13).to_dataframe()
print(df.to_string(index=False))

# ============================================================
# 1.4 Creación de vista v_resumen_ventas en dw_serving
# ============================================================
print("\n" + "=" * 60)
print("1.4 — Creando dw_serving.v_resumen_ventas...")
print("=" * 60)

query_vista = f"""
CREATE OR REPLACE VIEW `{PROYECTO}.dw_serving.v_resumen_ventas` AS
SELECT
    region,
    categoria,
    vendedor,
    COUNT(*) AS num_ventas,
    ROUND(SUM(total), 2) AS total_vendido,
    ROUND(AVG(total), 2) AS ticket_promedio
FROM `{PROYECTO}.dw_core.fact_ventas`
GROUP BY region, categoria, vendedor
"""

client.query(query_vista).result()
print("  Vista v_resumen_ventas creada exitosamente.")

# Consulta de evidencia 1.4
print("\n  --- Evidencia 1.4 ---")
query_evidencia_14 = f"""
SELECT * FROM `{PROYECTO}.dw_serving.v_resumen_ventas`
ORDER BY total_vendido DESC
LIMIT 5
"""
df2 = client.query(query_evidencia_14).to_dataframe()
print(df2.to_string(index=False))

print("\n" + "=" * 60)
print("Parte 1 completada exitosamente!")
print("=" * 60)
