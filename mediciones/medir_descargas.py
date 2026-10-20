#!/usr/bin/env python3
"""Medición 1: 20 descargas secuenciales contra 20 concurrentes.

Encola dos tareas 'descargar' con las mismas 20 URLs: una con
secuencial=true (un for con await adentro) y otra concurrente (TaskGroup).
Compara el tiempo de ejecución de cada una que reporta el servidor.

Uso (con el sistema levantado):
    docker compose exec api python -m mediciones.medir_descargas
    docker compose exec api python -m mediciones.medir_descargas --internet
"""
import argparse
import asyncio
import random
import sys

import httpx

from .comun import API, crear, esperar_tarea

# URLs públicas para la variante --internet
URLS_INTERNET = [
    'https://www.python.org', 'https://docs.python.org/3/',
    'https://fastapi.tiangolo.com', 'https://www.apache.org',
    'https://example.com', 'https://www.um.edu.ar', 'https://github.com',
    'https://www.google.com', 'https://www.cloudflare.com',
    'https://www.mozilla.org', 'https://www.debian.org',
    'https://www.kernel.org', 'https://pypi.org', 'https://ubuntu.com',
    'https://www.python-httpx.org', 'https://docs.docker.com',
    'https://www.rust-lang.org', 'https://go.dev', 'https://www.npmjs.com',
    'https://www.postgresql.org',
]


def urls_locales(cantidad, semilla=7):
    """URLs del servidor de prueba, con demoras al azar entre 0.2 y 1.2 s."""
    rnd = random.Random(semilla)
    return [f'http://prueba:8001/demora?seg={rnd.uniform(0.2, 1.2):.2f}&kb=50'
            for _ in range(cantidad)]


async def una_corrida(cliente, urls, secuencial):
    tid = await crear(cliente, 'descargar',
                      {'urls': urls, 'secuencial': secuencial}, timeout=300)
    tarea = await esperar_tarea(cliente, tid)
    res = tarea['resultado']
    tiempos = [d['segundos'] for d in res['descargas']]
    return res['total_s'], sum(tiempos), max(tiempos), res['errores']


async def medir(args):
    urls = URLS_INTERNET[:args.cantidad] if args.internet else urls_locales(
        args.cantidad)
    origen = 'internet' if args.internet else 'servidor de prueba local'
    print(f'{len(urls)} descargas ({origen}), {args.repeticiones} repeticiones\n')
    print(f'{"rep":>3}  {"secuencial":>11}  {"concurrente":>11}  '
          f'{"speedup":>7}  {"más lenta":>9}  {"errores":>7}')

    seq_total, con_total = [], []
    async with httpx.AsyncClient(base_url=args.api, timeout=30) as cliente:
        for i in range(args.repeticiones):
            # Se alterna el orden: con internet, la primera corrida paga DNS y
            # handshakes TLS que la segunda puede reutilizar (keep-alive)
            orden = [True, False] if i % 2 == 0 else [False, True]
            r = {}
            for secuencial in orden:
                r[secuencial] = await una_corrida(cliente, urls, secuencial)
            seq, con = r[True][0], r[False][0]
            seq_total.append(seq)
            con_total.append(con)
            print(f'{i + 1:>3}  {seq:>10.2f}s  {con:>10.2f}s  {seq / con:>6.1f}x'
                  f'  {r[False][2]:>8.2f}s  {r[True][3] + r[False][3]:>7}')

    seq = sum(seq_total) / len(seq_total)
    con = sum(con_total) / len(con_total)
    print(f'\npromedio  secuencial {seq:.2f}s  concurrente {con:.2f}s  '
          f'speedup {seq / con:.1f}x')


def crear_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--api', default=API)
    parser.add_argument('--cantidad', type=int, default=20)
    parser.add_argument('--repeticiones', type=int, default=3)
    parser.add_argument('--internet', action='store_true',
                        help='usar sitios reales en vez del servidor de prueba')
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
