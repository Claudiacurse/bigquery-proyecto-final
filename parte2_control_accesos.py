"""
Parte 2 — Control de Accesos
==============================
Ejecutar con: python parte2_control_accesos.py

Pasos:
  2.1 Crear service accounts + asignar BigQuery Job User
  2.1 Asignar permisos a nivel de dataset (Python BigQuery API)
  2.2 Generar llaves JSON
"""

import subprocess
from google.cloud import bigquery

PROYECTO = 'clau-personal'

# ============================================================
# 2.1 — Crear service accounts
# ============================================================
print("=" * 60)
print("2.1 — Creando service accounts...")
print("=" * 60)

for sa_name, display in [('analista-ventas-sa', 'Analista de Ventas'), ('data-engineer-sa', 'Data Engineer')]:
    result = subprocess.run([
        'gcloud', 'iam', 'service-accounts', 'create', sa_name,
        f'--display-name={display}',
        f'--project={PROYECTO}'
    ], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  {sa_name} creada")
    else:
        print(f"  {sa_name} ya existe")

# ============================================================
# 2.1 — Asignar BigQuery Job User a nivel de proyecto
# ============================================================
print("\n" + "=" * 60)
print("2.1 — Asignando BigQuery Job User a nivel de proyecto...")
print("=" * 60)

for sa_name in ['analista-ventas-sa', 'data-engineer-sa']:
    subprocess.run([
        'gcloud', 'projects', 'add-iam-policy-binding', PROYECTO,
        f'--member=serviceAccount:{sa_name}@{PROYECTO}.iam.gserviceaccount.com',
        '--role=roles/bigquery.jobUser',
        '--quiet'
    ], capture_output=True, text=True)
    print(f"  BigQuery Job User asignado a {sa_name}")

# ============================================================
# 2.1 — Asignar permisos a nivel de dataset (Python API)
# ============================================================
print("\n" + "=" * 60)
print("2.1 — Asignando permisos a nivel de dataset...")
print("=" * 60)

client = bigquery.Client(project=PROYECTO)

# analista-ventas-sa: BigQuery Data Viewer en dw_serving
ds_serving = client.get_dataset(f'{PROYECTO}.dw_serving')
access = list(ds_serving.access_entries)
entry = bigquery.AccessEntry(
    role='READER',
    entity_type='userByEmail',
    entity_id=f'analista-ventas-sa@{PROYECTO}.iam.gserviceaccount.com'
)
if entry not in access:
    access.append(entry)
    ds_serving.access_entries = access
    client.update_dataset(ds_serving, ['access_entries'])
print("  analista-ventas-sa -> BigQuery Data Viewer en dw_serving")

# data-engineer-sa: BigQuery Data Editor en dw_staging
ds_staging = client.get_dataset(f'{PROYECTO}.dw_staging')
access = list(ds_staging.access_entries)
entry = bigquery.AccessEntry(
    role='WRITER',
    entity_type='userByEmail',
    entity_id=f'data-engineer-sa@{PROYECTO}.iam.gserviceaccount.com'
)
if entry not in access:
    access.append(entry)
    ds_staging.access_entries = access
    client.update_dataset(ds_staging, ['access_entries'])
print("  data-engineer-sa -> BigQuery Data Editor en dw_staging")

# ============================================================
# 2.2 — Generar llaves JSON
# ============================================================
print("\n" + "=" * 60)
print("2.2 — Generando llaves JSON...")
print("=" * 60)

for sa_name in ['analista-ventas-sa', 'data-engineer-sa']:
    key_file = f'{sa_name}-key.json'
    result = subprocess.run([
        'gcloud', 'iam', 'service-accounts', 'keys', 'create', key_file,
        f'--iam-account={sa_name}@{PROYECTO}.iam.gserviceaccount.com',
        f'--project={PROYECTO}'
    ], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  {key_file} generada")
    else:
        print(f"  Error generando {key_file}: {result.stderr[:80]}")

print("\n" + "=" * 60)
print("Parte 2 — Setup completado!")
print("=" * 60)
print("\nAhora ejecuta el script de validación:")
print("  python parte2_validacion.py")
