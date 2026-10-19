"""Volcado del estado a JSON al apagar, y reporte del volcado previo al arrancar.

Son funciones sincrónicas (leen y escriben archivos): se llaman con
asyncio.to_thread para no bloquear el event loop.
"""
import json
import logging
import os
import time
from collections import Counter
from pathlib import Path

log = logging.getLogger('persistencia')


def volcar(directorio, tareas, estadisticas):
    """Escribe estado_<timestamp>.json y devuelve su ruta."""
    carpeta = Path(directorio)
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f'estado_{time.strftime("%Y%m%d_%H%M%S")}.json'
    temporal = destino.with_suffix('.tmp')

    contenido = {
        'volcado': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        'estadisticas': estadisticas,
        'tareas': tareas,
    }
    # Escribir a un temporal y renombrar: os.replace es atómico, así que un
    # corte a mitad de la escritura no deja un JSON truncado con el nombre final
    with open(temporal, 'w', encoding='utf-8') as f:
        json.dump(contenido, f, ensure_ascii=False, indent=2)
    os.replace(temporal, destino)
    return destino


def reportar_volcado_previo(directorio):
    """Si hay volcados de una ejecución anterior, informa el más reciente."""
    volcados = sorted(Path(directorio).glob('estado_*.json'))
    if not volcados:
        log.info('No hay volcados previos en %s/', directorio)
        return
    ultimo = volcados[-1]
    try:
        with open(ultimo, encoding='utf-8') as f:
            datos = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning('Volcado previo %s ilegible: %s', ultimo.name, e)
        return
    estados = Counter(t['estado'] for t in datos.get('tareas', []))
    log.info('Volcado previo encontrado: %s (%d en total en la carpeta). '
             'Tenía %d tareas: %s', ultimo.name, len(volcados),
             sum(estados.values()), dict(estados) or 'ninguna')
