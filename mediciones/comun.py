"""Funciones compartidas por los scripts de medición (clientes de la API)."""
import asyncio
import statistics

API = 'http://localhost:8000'
TERMINALES = {'completada', 'fallida', 'cancelada'}


async def crear(cliente, tipo, parametros, timeout=None):
    cuerpo = {'tipo': tipo, 'parametros': parametros}
    if timeout:
        cuerpo['timeout'] = timeout
    r = await cliente.post('/tareas', json=cuerpo)
    r.raise_for_status()
    return r.json()['id']


async def esperar_tarea(cliente, tarea_id, intervalo=0.05):
    while True:
        tarea = (await cliente.get(f'/tareas/{tarea_id}')).json()
        if tarea['estado'] in TERMINALES:
            return tarea
        await asyncio.sleep(intervalo)


async def esperar_todas(cliente, ids, intervalo=0.2):
    """Espera a que terminen todas, consultando una sola vez por vuelta."""
    pendientes = set(ids)
    while True:
        tareas = (await cliente.get('/tareas')).json()
        terminadas = {t['id']: t for t in tareas
                      if t['id'] in pendientes and t['estado'] in TERMINALES}
        if len(terminadas) == len(pendientes):
            return list(terminadas.values())
        await asyncio.sleep(intervalo)


async def config_servidor(cliente):
    e = (await cliente.get('/estadisticas')).json()
    return {'workers': e['workers']['total'],
            'procesos': e['executor']['procesos'],
            'hash_modo': e['executor']['hash_modo'],
            'cola': e['cola']['capacidad']}


def percentil(valores, p):
    if not valores:
        return 0.0
    if len(valores) == 1:
        return valores[0]
    return statistics.quantiles(valores, n=100, method='inclusive')[p - 1]
