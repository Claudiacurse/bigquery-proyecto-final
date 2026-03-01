"""
Parte 2.3 — Validación de permisos con Python
==============================================
Ejecutar con: python parte2_validacion.py

Este es el script exacto del enunciado.
Verifica que cada service account tiene los permisos correctos.
"""

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Forbidden, NotFound

PROYECTO = 'clau-personal'

def crear_cliente(ruta_json):
    creds = service_account.Credentials.from_service_account_file(
        ruta_json,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    return bigquery.Client(project=PROYECTO, credentials=creds)

def probar(cliente, descripcion, query):
    print(f'  {descripcion}')
    try:
        df = cliente.query(query).to_dataframe()
        print(f'    EXITO — {len(df)} fila(s)')
    except (Forbidden, NotFound) as e:
        print(f'    ACCESO DENEGADO — {str(e)[:80]}')
    except Exception as e:
        print(f'    ERROR — {str(e)[:80]}')

# Perfil: analista-ventas-sa
print('--- analista-ventas-sa ---')
c1 = crear_cliente('analista-ventas-sa-key.json')
probar(c1, 'Leer dw_serving [DEBE FUNCIONAR]',
       f'SELECT COUNT(*) FROM `{PROYECTO}.dw_serving.v_resumen_ventas`')
probar(c1, 'Leer dw_staging [DEBE FALLAR]',
       f'SELECT COUNT(*) FROM `{PROYECTO}.dw_staging.stg_ventas`')

# Perfil: data-engineer-sa
print('--- data-engineer-sa ---')
c2 = crear_cliente('data-engineer-sa-key.json')
probar(c2, 'Leer dw_staging [DEBE FUNCIONAR]',
       f'SELECT COUNT(*) FROM `{PROYECTO}.dw_staging.stg_ventas`')
probar(c2, 'Leer dw_serving [DEBE FALLAR]',
       f'SELECT COUNT(*) FROM `{PROYECTO}.dw_serving.v_resumen_ventas`')
