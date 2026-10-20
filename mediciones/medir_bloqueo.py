#!/usr/bin/env python3
"""Medición 2: cuánto tarda GET /tareas mientras corre un hashear pesado.

Hay que correrlo dos veces, levantando el servidor en cada modo:

    HASH_MODO=directo docker compose up -d --build     # sin executor
    docker compose exec api python -m mediciones.medir_bloqueo

    HASH_MODO=proceso docker compose up -d             # con executor
    docker compose exec api python -m mediciones.medir_bloqueo
"""
import argparse
import asyncio
import statistics
import sys
import time

import httpx

from .comun import API, config_servidor, crear, esperar_tarea, percentil


async def latencias_get(cliente, cantidad, pausa):
    muestras = []
    for _ in range(cantidad):
        t0 = time.perf_counter()
        (await cliente.get('/tareas')).raise_for_status()
        muestras.append((time.perf_counter() - t0) * 1000)
        await asyncio.sleep(pausa)
    return muestras


async def medir(args):
    async with httpx.AsyncClient(base_url=args.api, timeout=120) as cliente:
        cfg = await config_servidor(cliente)
        print(f'Servidor en HASH_MODO={cfg["hash_modo"]}, '
              f'{args.iteraciones:,} iteraciones de SHA-256\n')

        # Calentamiento: el pool de procesos arranca sus hijos recién cuando
        # recibe trabajo, y no queremos medir ese arranque
        await esperar_tarea(cliente, await crear(
            cliente, 'hashear', {'texto': 'calentar', 'iteraciones': 10_000}))

        base = await latencias_get(cliente, 20, args.pausa)

        tid = await crear(cliente, 'hashear',
                          {'texto': 'medicion', 'iteraciones': args.iteraciones},
                          timeout=600)
        t0 = time.perf_counter()
        durante = []
        # GET /tareas uno detrás del otro mientras la tarea siga corriendo
        while True:
            t1 = time.perf_counter()
            tareas = (await cliente.get('/tareas')).json()
            durante.append((time.perf_counter() - t1) * 1000)
            estado = next(t['estado'] for t in tareas if t['id'] == tid)
            if estado not in ('pendiente', 'ejecutando'):
                break
            await asyncio.sleep(args.pausa)
        total = time.perf_counter() - t0
        tarea = (await cliente.get(f'/tareas/{tid}')).json()

    def fila(nombre, m):
        print(f'  {nombre:<22} {len(m):>5} {statistics.mean(m):>9.1f} '
              f'{statistics.median(m):>9.1f} {percentil(m, 95):>9.1f} '
              f'{max(m):>9.1f}')

    print(f'  {"latencia GET /tareas":<22} {"n":>5} {"media ms":>9} '
          f'{"mediana":>9} {"p95":>9} {"máx ms":>9}')
    fila('en reposo', base)
    fila('durante el hash', durante)
    print(f'\n  hash: {tarea["estado"]}, ejecución {tarea["ejecucion_s"]:.2f}s '
          f'(observado desde el cliente: {total:.2f}s)')


def crear_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--api', default=API)
    parser.add_argument('--iteraciones', type=int, default=15_000_000)
    parser.add_argument('--pausa', type=float, default=0.05,
                        help='segundos entre GET y GET')
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
