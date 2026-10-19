"""El corazón del sistema: registro de tareas, cola y pool de workers.

Todo corre en el hilo del event loop. Ninguna estructura de acá tiene Lock:
el loop solo cambia de corrutina en un await, y ninguna sección que lee y
modifica el estado tiene un await en el medio.
"""
import asyncio
import itertools
import logging
from collections import Counter

from . import tareas
from .modelos import TERMINALES, Estado, Tarea

log = logging.getLogger('plataforma')

# Marca de "no hay más trabajo" para apagar a los workers (clase 21).
# object() es único: ninguna tarea que venga de afuera puede ser idéntica a él.
CENTINELA = object()


class ColaLlena(Exception):
    pass


class NoAcepta(Exception):
    pass


class Plataforma:
    def __init__(self, config, cliente_http, pool):
        self.config = config
        self.cliente_http = cliente_http      # httpx.AsyncClient compartido
        self.pool = pool                      # ProcessPoolExecutor compartido
        self.cola = asyncio.Queue(maxsize=config.cola_max)
        self.tareas: dict[int, Tarea] = {}
        self.workers: dict[int, asyncio.Task] = {}
        self.ocupados = 0
        self.aceptando = True
        self.cerrando = False
        self._apagado_listo = asyncio.Event()
        self._ids = itertools.count(1)

    # ------------------------------------------------------------ arranque

    def iniciar(self):
        for wid in range(self.config.workers):
            self._lanzar_worker(wid)
        log.info('%d workers listos, cola de %d lugares, hashear en modo %r',
                 self.config.workers, self.config.cola_max,
                 self.config.hash_modo)

    def _lanzar_worker(self, wid):
        # Guardamos la referencia: asyncio solo guarda referencias débiles a
        # las tasks, y una task sin referencia puede ser recolectada.
        w = asyncio.create_task(self._worker(wid), name=f'worker-{wid}')
        w.add_done_callback(lambda t: self._worker_termino(wid, t))
        self.workers[wid] = w

    def _worker_termino(self, wid, w):
        """Si un worker muere por una excepción no prevista, se reemplaza.

        Es el supervisor que repone workers caídos (clase 6, ejercicio 6),
        con un callback en vez de SIGCHLD.
        """
        if w.cancelled():
            return
        exc = w.exception()      # leerla evita "Task exception was never retrieved"
        if exc is None or self.cerrando:
            return               # salió por el centinela, o estamos apagando
        log.error('worker-%d murió por %r; lanzando reemplazo', wid, exc)
        self._lanzar_worker(wid)

    # ------------------------------------------------------------ API pública

    def encolar(self, tipo, parametros, timeout):
        """Registra la tarea y la mete en la cola. No espera nada: vuelve ya."""
        if not self.aceptando:
            raise NoAcepta()
        tarea = Tarea(id=next(self._ids), tipo=tipo, parametros=parametros,
                      timeout=timeout or self.config.timeout_defecto)
        try:
            # put_nowait y no await put(): si la cola está llena preferimos
            # rechazar con 503 a dejar al cliente HTTP colgado esperando lugar
            self.cola.put_nowait(tarea)
        except asyncio.QueueFull:
            raise ColaLlena() from None
        self.tareas[tarea.id] = tarea
        return tarea

    def listar(self, estado=None):
        return [t for t in self.tareas.values()
                if estado is None or t.estado == estado]

    async def cancelar(self, tarea, espera_max=5.0):
        """Cancela una tarea pendiente o en ejecución.

        Devuelve True si al volver ya quedó cancelada.
        """
        if tarea.estado is Estado.PENDIENTE:
            # Sigue dentro de la asyncio.Queue, que no permite sacar un
            # elemento del medio. Se marca, y el worker que la saque la saltea.
            tarea.cancelar()
            return True

        if tarea.estado is Estado.EJECUTANDO and tarea.trabajo is not None:
            trabajo = tarea.trabajo
            # cancel() no detiene nada en el momento: inyecta CancelledError
            # en el próximo await del trabajo (clase 20). Esperamos un poco a
            # que llegue para que la respuesta muestre el estado final.
            trabajo.cancel()
            await asyncio.wait([trabajo], timeout=espera_max)
            return tarea.estado is Estado.CANCELADA
        return False

    def estadisticas(self):
        por_estado = Counter(t.estado.value for t in self.tareas.values())
        terminadas = [t for t in self.tareas.values()
                      if t.estado in TERMINALES and t.ejecucion_s is not None]
        iniciadas = [t for t in self.tareas.values() if t.espera_s is not None]

        por_tipo = {}
        for tipo in ('descargar', 'hashear', 'esperar'):
            tiempos = [t.ejecucion_s for t in terminadas if t.tipo == tipo]
            por_tipo[tipo] = _promedio(tiempos)

        return {
            'total': len(self.tareas),
            'por_estado': {e.value: por_estado.get(e.value, 0) for e in Estado},
            'tiempos_promedio_s': {
                'espera_en_cola': _promedio([t.espera_s for t in iniciadas]),
                'ejecucion': _promedio([t.ejecucion_s for t in terminadas]),
                'ejecucion_por_tipo': por_tipo,
            },
            'workers': {
                'total': len(self.workers),
                'ocupados': self.ocupados,
                'libres': len(self.workers) - self.ocupados,
            },
            'cola': {'en_espera': self.cola.qsize(),
                     'capacidad': self.config.cola_max},
            'executor': {'procesos': self.config.procesos,
                         'hash_modo': self.config.hash_modo},
            'aceptando': self.aceptando,
        }

    # ------------------------------------------------------------ workers

    async def _worker(self, wid):
        """Consumidor: el patrón productor-consumidor de la clase 11."""
        while True:
            tarea = await self.cola.get()
            if tarea is CENTINELA:
                self.cola.task_done()
                return                                 # salida ordenada
            try:
                if tarea.estado is Estado.PENDIENTE:   # si no, la cancelaron
                    await self._ejecutar(tarea, wid)
            except Exception as e:
                # Un bug del propio worker (los errores de la tarea ya los
                # maneja _correr). La tarea no puede quedar colgada en un
                # estado intermedio; el worker muere y _worker_termino lo
                # reemplaza.
                if tarea.estado not in TERMINALES:
                    tarea.fallar(f'error interno del worker: {e!r}')
                raise
            finally:
                self.cola.task_done()

    async def _ejecutar(self, tarea, wid):
        tarea.iniciar(wid)
        self.ocupados += 1
        # El trabajo corre en su propia Task para poder cancelarlo (DELETE)
        # sin matar al worker, que tiene que seguir consumiendo de la cola.
        trabajo = asyncio.create_task(self._correr(tarea),
                                      name=f'tarea-{tarea.id}')
        tarea.trabajo = trabajo
        try:
            # asyncio.wait no propaga la excepción ni la cancelación del
            # trabajo: el worker sobrevive a lo que le pase a la tarea.
            await asyncio.wait([trabajo])
        except asyncio.CancelledError:
            # Cancelaron al WORKER (apagado forzado): no dejar el trabajo
            # huérfano. Se cancela, se espera a que termine, y se relanza.
            trabajo.cancel()
            await asyncio.wait([trabajo])
            raise
        finally:
            self.ocupados -= 1
            tarea.trabajo = None
            if tarea.estado is Estado.EJECUTANDO:
                # El cancel() llegó antes de que el trabajo arrancara: la
                # corrutina nunca ejecutó su except, así que se marca acá.
                tarea.cancelar()

    async def _correr(self, tarea):
        """Ejecuta una tarea con su timeout y registra cómo terminó."""
        try:
            async with asyncio.timeout(tarea.timeout):
                resultado = await self._despachar(tarea)
        except TimeoutError:
            # asyncio.timeout implementa el timeout CANCELANDO la tarea y
            # convirtiendo ese CancelledError en TimeoutError
            tarea.fallar(f'timeout: superó {tarea.timeout} s')
        except asyncio.CancelledError:
            tarea.cancelar()
            raise          # siempre relanzarla: si no, la cancelación "no pasó"
        except Exception as e:
            log.warning('tarea %d falló: %r', tarea.id, e)
            tarea.fallar(f'{type(e).__name__}: {e}')
        else:
            tarea.completar(resultado)

    def _despachar(self, tarea):
        p = tarea.parametros
        if tarea.tipo == 'descargar':
            return tareas.descargar(p, self.cliente_http)
        if tarea.tipo == 'hashear':
            return tareas.hashear(p, self.pool, self.config.hash_modo,
                                  self.config.hash_bloque)
        if tarea.tipo == 'esperar':
            return tareas.esperar(p)
        raise ValueError(f'tipo desconocido: {tarea.tipo}')

    # ------------------------------------------------------------ apagado

    async def apagar(self):
        """Apagado ordenado. Es idempotente: la segunda llamada solo espera."""
        if self.cerrando:
            await self._apagado_listo.wait()
            return
        self.cerrando = True
        self.aceptando = False                 # 1. no más tareas nuevas
        limite = self.config.limite_apagado

        # 2. Las pendientes no llegaron a empezar: se cancelan sin ejecutarse.
        # Siguen en la cola, pero el worker que las saque las saltea.
        pendientes = self.listar(Estado.PENDIENTE)
        for t in pendientes:
            t.cancelar('apagado del servidor antes de ejecutarse')
        en_curso = len(self.listar(Estado.EJECUTANDO))
        log.info('Apagando: %d pendientes canceladas, esperando hasta %.0f s '
                 'a %d en curso', len(pendientes), limite, en_curso)

        # 3. Un centinela por worker, al final de la cola. Cada worker termina
        # la tarea que tiene en la mano, saltea las canceladas, toma un
        # centinela y sale. Todo con un límite de tiempo.
        workers = list(self.workers.values())
        try:
            async with asyncio.timeout(limite):
                for _ in workers:
                    await self.cola.put(CENTINELA)   # espera si la cola está llena
                # asyncio.wait no cancela a los workers si vence el timeout:
                # eso lo decidimos nosotros abajo
                await asyncio.wait(workers)
        except TimeoutError:
            # 4. Las que no terminaron a tiempo se cancelan. Cancelar al
            # worker cancela también su trabajo (ver _ejecutar).
            demoradas = self.listar(Estado.EJECUTANDO)
            log.warning('%d tareas no terminaron a tiempo: se cancelan',
                        len(demoradas))
            for t in demoradas:
                t.motivo_cancelacion = 'apagado del servidor (límite vencido)'
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        self._apagado_listo.set()
        log.info('Workers detenidos')


def _promedio(valores):
    valores = [v for v in valores if v is not None]
    return round(sum(valores) / len(valores), 4) if valores else None
