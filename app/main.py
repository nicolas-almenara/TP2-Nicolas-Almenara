#!/usr/bin/env python3
"""API HTTP de la plataforma de tareas distribuidas.

Uso:
    python -m app.main                 # lo que hace el contenedor
    docker compose up --build          # la forma normal de levantarla

Después:
    http://localhost:8000/docs         <- la documentación se genera sola
"""
import asyncio
import logging
import multiprocessing
import signal
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response, status

from . import persistencia
from .config import config
from .modelos import Estado, PedidoTarea, TareaCreada, TareaRespuesta
from .plataforma import ColaLlena, NoAcepta, Plataforma

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-7s %(name)s: %(message)s')
log = logging.getLogger('api')


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    # ---------------- al arrancar
    await asyncio.to_thread(persistencia.reportar_volcado_previo,
                            config.dir_volcados)

    # 'spawn' (clase 8): el hijo arranca un intérprete limpio en vez de
    # heredar con fork() el estado del loop y de los threads (un fork después
    # de crear threads puede heredar un lock tomado). forkserver también lo
    # evitaría, pero no existe en Windows y spawn anda en todas partes.
    pool = ProcessPoolExecutor(max_workers=config.procesos,
                               mp_context=multiprocessing.get_context('spawn'))
    # Un solo cliente HTTP para todas las descargas: reutiliza conexiones
    # (keep-alive, clase 19). Su pool acota cuántas conexiones hay abiertas a
    # la vez: es el Semaphore de la clase 20, pero global. Las descargas que
    # no consiguen conexión esperan su turno (pool=None: sin límite propio;
    # las acota el timeout de la tarea).
    cliente = httpx.AsyncClient(
        timeout=httpx.Timeout(config.timeout_descarga, pool=None),
        limits=httpx.Limits(max_connections=config.conexiones_max),
        follow_redirects=True)
    plataforma = Plataforma(config, cliente, pool)
    plataforma.iniciar()
    app.state.plataforma = plataforma

    loop = asyncio.get_running_loop()
    servidor = getattr(app.state, 'servidor', None)
    if servidor is not None:
        # Señales manejadas por el loop (clase 20): el callback corre entre
        # tareas, en un momento seguro, y no en medio de cualquier instrucción
        # como un handler de signal.signal() (clase 6).
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _al_recibir_senal, sig, app)

    yield   # ---------------- acá el servidor atiende pedidos

    # ---------------- al cerrar
    await plataforma.apagar()     # no hace nada si ya lo hizo la señal
    await _volcar(plataforma)
    if servidor is not None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    await cliente.aclose()
    # shutdown(wait=True) bloquea hasta que los procesos terminen: a un thread
    await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=True)
    log.info('Apagado completo')


def _al_recibir_senal(sig, app):
    plataforma = app.state.plataforma
    nombre = signal.Signals(sig).name
    if plataforma.cerrando:
        log.info('%s recibida, el apagado ya está en curso', nombre)
        return
    log.info('%s recibida: apagado ordenado', nombre)
    # Guardar la referencia a la task para que no la recolecten
    app.state.apagado = asyncio.create_task(_apagado_ordenado(app))


async def _apagado_ordenado(app):
    try:
        # Durante el apagado la API sigue respondiendo: los GET muestran
        # cómo terminan las tareas, y los POST reciben 503.
        await app.state.plataforma.apagar()
    finally:
        # Recién ahora uvicorn deja de atender. Después corre la parte final
        # del lifespan, que vuelca el estado a JSON.
        app.state.servidor.should_exit = True


async def _volcar(plataforma):
    datos = [t.a_dict() for t in plataforma.tareas.values()]
    ruta = await asyncio.to_thread(persistencia.volcar, config.dir_volcados,
                                   datos, plataforma.estadisticas())
    log.info('Estado volcado en %s (%d tareas)', ruta, len(datos))


app = FastAPI(
    title='Plataforma de tareas distribuidas',
    description='TP2 de Computación II: encola tareas (descargar, hashear, '
                'esperar) y las ejecuta un pool de workers asyncio.',
    lifespan=ciclo_de_vida,
)


def _plataforma(request: Request) -> Plataforma:
    return request.app.state.plataforma


def _buscar(request, tarea_id):
    tarea = _plataforma(request).tareas.get(tarea_id)
    if tarea is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f'No existe la tarea {tarea_id}')
    return tarea


# Todos los endpoints son async def y ninguno bloquea: solo tocan
# estructuras en memoria. El trabajo de verdad lo hacen los workers.

@app.post('/tareas', response_model=TareaCreada,
          status_code=status.HTTP_202_ACCEPTED)
async def crear_tarea(pedido: PedidoTarea, request: Request):
    """Encola la tarea y responde enseguida, sin esperar a que se ejecute.

    202 Accepted: el pedido quedó aceptado para procesarse más tarde.
    503 si la cola está llena o el servidor se está apagando.
    """
    try:
        tarea = _plataforma(request).encolar(
            pedido.tipo, pedido.parametros.model_dump(mode='json'),
            pedido.timeout)
    except NoAcepta:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            'El servidor se está apagando') from None
    except ColaLlena:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            'La cola está llena, reintentá más tarde') from None
    return {'id': tarea.id, 'estado': tarea.estado.value}


@app.get('/tareas', response_model=list[TareaRespuesta])
async def listar_tareas(
    request: Request,
    estado: Estado | None = Query(default=None,
                                  description='Filtrar por estado'),
):
    """Lista las tareas, opcionalmente filtradas por estado."""
    return [t.a_dict() for t in _plataforma(request).listar(estado)]


@app.get('/tareas/{tarea_id}', response_model=TareaRespuesta)
async def obtener_tarea(tarea_id: int, request: Request):
    """Estado y resultado de una tarea. 404 si no existe."""
    return _buscar(request, tarea_id).a_dict()


@app.delete('/tareas/{tarea_id}', response_model=TareaRespuesta)
async def cancelar_tarea(tarea_id: int, request: Request, response: Response):
    """Cancela una tarea pendiente o en ejecución.

    200 con la tarea ya cancelada; 202 si la cancelación se pidió pero
    todavía no llegó a un await; 409 si la tarea ya había terminado.
    """
    tarea = _buscar(request, tarea_id)
    if tarea.estado not in (Estado.PENDIENTE, Estado.EJECUTANDO):
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f'La tarea ya terminó ({tarea.estado.value})')
    cancelada = await _plataforma(request).cancelar(tarea)
    if not cancelada:
        # Se pidió la cancelación pero todavía no llegó a un await
        response.status_code = status.HTTP_202_ACCEPTED
    return tarea.a_dict()


@app.get('/estadisticas')
async def estadisticas(request: Request):
    """Totales por estado, tiempos promedio, workers ocupados y cola."""
    return _plataforma(request).estadisticas()


def main():
    servidor = uvicorn.Server(uvicorn.Config(
        app, host='0.0.0.0', port=config.puerto, timeout_graceful_shutdown=5))
    # Con la referencia al servidor, el lifespan instala nuestros manejadores
    # de señales y decide cuándo uvicorn deja de atender
    app.state.servidor = servidor
    servidor.run()


# La guarda es obligatoria: con 'spawn', cada proceso del pool re-importa
# este módulo, y sin ella cada uno intentaría levantar otro servidor.
if __name__ == '__main__':
    main()
