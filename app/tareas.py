"""Los tres tipos de tarea.

- descargar: I/O-bound. Corre en el event loop con await.
- hashear:   CPU-bound. Sale del loop con un executor.
- esperar:   I/O simulado con asyncio.sleep.
"""
import asyncio
import hashlib
import time

import httpx


# ---------------------------------------------------------------- descargar

async def _descargar_una(cliente, url):
    """Descarga una URL y devuelve código, tamaño y tiempo.

    Los errores de red se devuelven como dato y no se propagan: que falle una
    URL no tiene por qué cancelar a las demás. CancelledError NO se captura
    (no es Exception): si cancelan la tarea, tiene que llegar hasta arriba.
    """
    t0 = time.perf_counter()
    try:
        tamano = 0
        # stream: se cuentan los bytes a medida que llegan, sin guardar el
        # cuerpo en memoria. El 'async with' cierra la conexión aunque nos
        # cancelen a mitad de la descarga.
        async with cliente.stream('GET', url) as respuesta:
            async for bloque in respuesta.aiter_bytes():
                tamano += len(bloque)
        return {
            'url': url,
            'codigo': respuesta.status_code,
            'bytes': tamano,
            'segundos': round(time.perf_counter() - t0, 4),
        }
    except httpx.HTTPError as e:
        return {
            'url': url,
            'error': f'{type(e).__name__}: {e}',
            'segundos': round(time.perf_counter() - t0, 4),
        }


async def descargar(params, cliente):
    urls = [str(u) for u in params['urls']]
    t0 = time.perf_counter()

    if params.get('secuencial'):
        # Un for con await adentro: cada descarga empieza cuando termina la
        # anterior. Existe solo para comparar en la medición 1.
        resultados = [await _descargar_una(cliente, u) for u in urls]
    else:
        # TaskGroup (clase 21): todas a la vez. Si la tarea se cancela, el
        # grupo cancela a todas las descargas hijas y espera a que terminen:
        # no quedan corrutinas huérfanas.
        async with asyncio.TaskGroup() as grupo:
            hijas = [grupo.create_task(_descargar_una(cliente, u)) for u in urls]
        resultados = [h.result() for h in hijas]

    return {
        'modo': 'secuencial' if params.get('secuencial') else 'concurrente',
        'total_s': round(time.perf_counter() - t0, 4),
        'ok': sum(1 for r in resultados if 'error' not in r),
        'errores': sum(1 for r in resultados if 'error' in r),
        'descargas': resultados,
    }


# ------------------------------------------------------------------ hashear

def hash_iterado(datos: bytes, vueltas: int) -> bytes:
    """SHA-256 aplicado `vueltas` veces. CPU puro: no cede el control nunca.

    Está a nivel de módulo para que pickle la pueda mandar a otro proceso.
    """
    for _ in range(vueltas):
        datos = hashlib.sha256(datos).digest()
    return datos


async def hashear(params, pool, modo, bloque):
    restantes = params['iteraciones']
    datos = params['texto'].encode()
    t0 = time.perf_counter()

    if modo == 'directo':
        # MAL A PROPÓSITO: calcula dentro de la corrutina y congela el event
        # loop. Solo existe para la medición 2 (HASH_MODO=directo).
        datos = hash_iterado(datos, restantes)
    else:
        loop = asyncio.get_running_loop()
        # Se calcula por bloques. Entre bloque y bloque la corrutina vuelve
        # al loop, y ahí es donde puede llegarle un cancel() o un timeout.
        # Si fuera un solo bloque, el proceso seguiría calculando hasta el
        # final aunque cancelen la tarea.
        while restantes > 0:
            n = min(bloque, restantes)
            if modo == 'proceso':
                datos = await loop.run_in_executor(pool, hash_iterado, datos, n)
            else:
                datos = await asyncio.to_thread(hash_iterado, datos, n)
            restantes -= n

    return {
        'hash': datos.hex(),
        'iteraciones': params['iteraciones'],
        'modo': modo,
        'segundos': round(time.perf_counter() - t0, 4),
    }


# ------------------------------------------------------------------ esperar

async def esperar(params):
    t0 = time.perf_counter()
    await asyncio.sleep(params['segundos'])     # cede el control: no bloquea
    return {'durmio_s': round(time.perf_counter() - t0, 4)}
