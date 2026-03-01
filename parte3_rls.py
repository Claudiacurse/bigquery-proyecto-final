"""
Parte 3 — Seguridad a Nivel de Filas (Row-Level Security)
==========================================================
Ejecutar con: python parte3_rls.py

Pasos:
  3.1 Asignar Data Viewer a ambas service accounts en dw_core (requerido para RLS)
  3.1 Crear políticas RLS sobre fact_ventas
  3.2 Verificar el efecto del filtro con ambas service accounts

NOTA: La evidencia 3.1 (INFORMATION_SCHEMA.ROW_ACCESS_POLICIES) debe
ejecutarse directamente en la consola de BigQuery:

  SELECT policy_id, filter_predicate, ARRAY_TO_STRING(grantee_list, ', ') AS aplica_a
  FROM `clau-personal`.dw_core.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
  WHERE table_name = 'fact_ventas';
"""

from google.cloud import bigquery
from google.oauth2 import service_account

PROYECTO = 'clau-personal'

client = bigquery.Client(project=PROYECTO)

# ============================================================
# 3.1 — Dar acceso BigQuery Data Viewer en dw_core a ambas SA
# (necesario para que las políticas RLS funcionen)
# ============================================================
print("=" * 60)
print("3.1 — Asignando Data Viewer en dw_core a las service accounts...")
print("=" * 60)

ds_core = client.get_dataset(f'{PROYECTO}.dw_core')
access = list(ds_core.access_entries)

for sa_name in ['analista-ventas-sa', 'data-engineer-sa']:
    entry = bigquery.AccessEntry(
        role='READER',
        entity_type='userByEmail',
        entity_id=f'{sa_name}@{PROYECTO}.iam.gserviceaccount.com'
    )
    if entry not in access:
        access.append(entry)
        print(f"  {sa_name} -> Data Viewer en dw_core (agregado)")
    else:
        print(f"  {sa_name} -> Data Viewer en dw_core (ya existe)")

ds_core.access_entries = access
client.update_dataset(ds_core, ['access_entries'])

# ============================================================
# 3.1 — Crear políticas de acceso RLS
# ============================================================
print("\n" + "=" * 60)
print("3.1 — Creando políticas RLS sobre fact_ventas...")
print("=" * 60)

# Política: analista-ventas-sa solo ve Lima
query_rls_lima = f"""
CREATE OR REPLACE ROW ACCESS POLICY rls_lima
  ON `{PROYECTO}.dw_core.fact_ventas`
  GRANT TO ('serviceAccount:analista-ventas-sa@{PROYECTO}.iam.gserviceaccount.com')
  FILTER USING (region = 'Lima');
"""
client.query(query_rls_lima).result()
print("  Política rls_lima creada (analista-ventas-sa -> solo Lima)")

# Política: data-engineer-sa ve todo
query_rls_admin = f"""
CREATE OR REPLACE ROW ACCESS POLICY rls_admin
  ON `{PROYECTO}.dw_core.fact_ventas`
  GRANT TO ('serviceAccount:data-engineer-sa@{PROYECTO}.iam.gserviceaccount.com')
  FILTER USING (TRUE);
"""
client.query(query_rls_admin).result()
print("  Política rls_admin creada (data-engineer-sa -> todo)")

print("\n  NOTA: Para la evidencia 3.1, ejecuta este query en la consola de BigQuery:")
print(f"  SELECT policy_id, filter_predicate, ARRAY_TO_STRING(grantee_list, ', ') AS aplica_a")
print(f"  FROM `{PROYECTO}`.dw_core.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES")
print(f"  WHERE table_name = 'fact_ventas';")

# ============================================================
# 3.2 — Verificación del efecto del filtro
# ============================================================
print("\n" + "=" * 60)
print("3.2 — Verificando efecto del filtro RLS...")
print("=" * 60)

def crear_cliente(ruta_json):
    creds = service_account.Credentials.from_service_account_file(
        ruta_json,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    return bigquery.Client(project=PROYECTO, credentials=creds)

QUERY = f'''
    SELECT region, COUNT(*) AS filas, ROUND(SUM(total),2) AS ventas
    FROM `{PROYECTO}.dw_core.fact_ventas`
    GROUP BY region ORDER BY region
'''

print('\n--- analista-ventas-sa (politica: Lima) ---')
df1 = crear_cliente('analista-ventas-sa-key.json').query(QUERY).to_dataframe()
print(df1.to_string(index=False))

print('\n--- data-engineer-sa (politica: TRUE) ---')
df2 = crear_cliente('data-engineer-sa-key.json').query(QUERY).to_dataframe()
print(df2.to_string(index=False))

print("\n" + "=" * 60)
print("Parte 3 completada exitosamente!")
print("=" * 60)
