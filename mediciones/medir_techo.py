#!/usr/bin/env python3
"""Medición 3: el techo del pool.

Encola K tareas iguales de golpe, para K creciente, y mide cuánto tarda el
lote completo y cuánto esperó cada tarea en la cola. El techo aparece cuando
el tiempo total deja de ser el de una sola tarea y empieza a crecer.

Ejemplos:
    # esperar: el límite es la cantidad de workers
    docker compose exec api python -m mediciones.medir_techo --tipo esperar

    # hashear: con muchos workers, el límite pasan a ser los núcleos
    WORKERS=64 docker compose up -d
    docker compose exec api python -m mediciones.medir_techo --tipo hashear
"""
import argparse
import asyncio
import multiprocessing
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import httpx

from .comun import API, config_servidor, crear, esperar_todas, percentil


def parametros(args):
    if args.tipo == 'esperar':
        return {'segundos': args.segundos}
    if args.tipo == 'hashear':
        return {'texto': 'techo', 'iteraciones': args.iteraciones}
    return {'urls': [f'http://prueba:8001/demora?seg={args.segundos}&kb=10']}


def _enviar_parte(api, tipo, params, n):
    """Corre en un proceso aparte: manda n POST con su propio event loop."""
    async def enviar():
        # Pocos POST en vuelo por proceso: el costo de CPU de httpx por pedido
        # crece con la cantidad de pedidos simultáneos en su pool
        sem = asyncio.Semaphore(8)
        limites = httpx.Limits(max_connections=8)
        async with httpx.AsyncClient(base_url=api, timeout=120,
                                     limits=limites) as cliente:
            async def uno():
                async with sem:
                    return await crear(cliente, tipo, params, timeout=600)
            return await asyncio.gather(*(uno() for _ in range(n)),
                                        return_exceptions=True)

    respuestas = asyncio.run(enviar())
    ids = [r for r in respuestas if isinstance(r, int)]
    return ids, len(respuestas) - len(ids)


async def lote(cliente, pool, args, k):
    # El envío se reparte entre varios procesos cliente: medido con un solo
    # proceso, el script llegaba al 100% de CPU con el servidor al 20%, o sea
    # que estábamos midiendo el techo del cliente y no el del servidor
    loop = asyncio.get_running_loop()
    partes = [k // args.clientes + (1 if i < k % args.clientes else 0)
              for i in range(args.clientes)]
    t0 = time.perf_counter()
    resultados = await asyncio.gather(*(
        loop.run_in_executor(pool, _enviar_parte, args.api, args.tipo,
                             parametros(args), n)
        for n in partes if n))
    encolar = time.perf_counter() - t0
    ids = [i for parte, _ in resultados for i in parte]
    rechazadas = sum(r for _, r in resultados)

    tareas = await esperar_todas(cliente, ids) if ids else []
    total = time.perf_counter() - t0
    esperas = [t['espera_s'] for t in tareas if t['espera_s'] is not None]
    ejec = [t['ejecucion_s'] for t in tareas if t['ejecucion_s'] is not None]
    fallidas = sum(1 for t in tareas if t['estado'] != 'completada')
    return {
        'k': k, 'total': total, 'encolar': encolar,
        # Throughput, como en el benchmark de la clase 14
        'por_seg': (len(tareas) - fallidas) / total,
        'espera_media': statistics.mean(esperas) if esperas else 0,
        'espera_p95': percentil(esperas, 95),
        'ejec_media': statistics.mean(ejec) if ejec else 0,
        'rechazadas': rechazadas, 'fallidas': fallidas,
    }


async def medir(args):
    pool = ProcessPoolExecutor(args.clientes,
                               mp_context=multiprocessing.get_context('spawn'))
    async with httpx.AsyncClient(base_url=args.api, timeout=120) as cliente:
        cfg = await config_servidor(cliente)
        print(f'tipo={args.tipo}  workers={cfg["workers"]}  '
              f'procesos={cfg["procesos"]}  cola={cfg["cola"]}  '
              f'hash_modo={cfg["hash_modo"]}\n')
        # Calentamiento: que arranquen los procesos cliente antes de medir
        loop = asyncio.get_running_loop()
        await asyncio.gather(*(
            loop.run_in_executor(pool, _enviar_parte, args.api, args.tipo, {}, 0)
            for _ in range(args.clientes)))
        if args.tipo == 'hashear':
            # Calentamiento: que el pool arranque sus procesos antes de medir
            ids = await asyncio.gather(*(
                crear(cliente, 'hashear', {'texto': 'calentar',
                                           'iteraciones': 200_000})
                for _ in range(cfg['procesos'])))
            await esperar_todas(cliente, ids)
        print(f'{"K":>6} {"total s":>8} {"tareas/s":>9} {"encolar s":>9} '
              f'{"espera media":>12} {"espera p95":>10} {"ejecución":>9} '
              f'{"rechaz.":>7} {"fall.":>5}')
        for k in args.cantidades:
            r = await lote(cliente, pool, args, k)
            print(f'{r["k"]:>6} {r["total"]:>8.2f} {r["por_seg"]:>9.1f} '
                  f'{r["encolar"]:>9.2f} '
                  f'{r["espera_media"]:>12.2f} {r["espera_p95"]:>10.2f} '
                  f'{r["ejec_media"]:>9.2f} {r["rechazadas"]:>7} '
                  f'{r["fallidas"]:>5}')
    pool.shutdown()


def crear_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--api', default=API)
    parser.add_argument('--tipo', choices=['esperar', 'hashear', 'descargar'],
                        default='esperar')
    parser.add_argument('--cantidades',
                        type=lambda s: [int(x) for x in s.split(',')],
                        default=[1, 2, 4, 8, 16, 32, 64])
    parser.add_argument('--segundos', type=float, default=0.5)
    parser.add_argument('--iteraciones', type=int, default=1_000_000)
    parser.add_argument('--clientes', type=int, default=8,
                        help='procesos que mandan los POST en paralelo')
    return parser


def main():
    args = crear_parser().parse_args()
    try:
        asyncio.run(medir(args))
    except KeyboardInterrupt:
        print('\nInterrumpido', file=sys.stderr)
        sys.exit(130)
    except httpx.HTTPError as e:
        print(f'Error hablando con la API ({args.api}): {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
