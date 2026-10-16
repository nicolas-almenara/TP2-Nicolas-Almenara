"""Modelos: el registro interno de cada tarea y los cuerpos que acepta la API."""
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, HttpUrl


class Estado(str, Enum):
    PENDIENTE = 'pendiente'
    EJECUTANDO = 'ejecutando'
    COMPLETADA = 'completada'
    FALLIDA = 'fallida'
    CANCELADA = 'cancelada'


TERMINALES = {Estado.COMPLETADA, Estado.FALLIDA, Estado.CANCELADA}


def _iso(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass
class Tarea:
    """Estado de una tarea. Vive en memoria, en el dict de la plataforma."""
    id: int
    tipo: str
    parametros: dict
    timeout: float
    estado: Estado = Estado.PENDIENTE
    resultado: Any = None
    error: str | None = None
    worker: int | None = None
    # Fechas de reloj de pared, para mostrar (time.time)
    creada: float = field(default_factory=time.time)
    iniciada: float | None = None
    terminada: float | None = None
    # Instantes monotónicos, para medir duraciones: a diferencia de
    # time.time(), no salta si alguien ajusta el reloj del sistema
    _t_creada: float = field(default_factory=time.monotonic, repr=False)
    _t_iniciada: float | None = field(default=None, repr=False)
    _t_terminada: float | None = field(default=None, repr=False)
    # La asyncio.Task que ejecuta el trabajo; DELETE la cancela.
    # No se serializa: solo existe mientras la tarea está ejecutando.
    trabajo: asyncio.Task | None = field(default=None, repr=False)
    motivo_cancelacion: str = 'cancelada por el usuario'

    def iniciar(self, worker):
        self.estado = Estado.EJECUTANDO
        self.worker = worker
        self.iniciada = time.time()
        self._t_iniciada = time.monotonic()

    def completar(self, resultado):
        self._terminar(Estado.COMPLETADA)
        self.resultado = resultado

    def fallar(self, error):
        self._terminar(Estado.FALLIDA)
        self.error = error

    def cancelar(self, motivo=None):
        self._terminar(Estado.CANCELADA)
        self.error = motivo or self.motivo_cancelacion

    def _terminar(self, estado):
        self.estado = estado
        self.terminada = time.time()
        self._t_terminada = time.monotonic()

    @property
    def espera_s(self):
        """Tiempo que pasó en la cola antes de que un worker la tomara."""
        if self._t_iniciada is None:
            return None
        return round(self._t_iniciada - self._t_creada, 4)

    @property
    def ejecucion_s(self):
        if self._t_iniciada is None or self._t_terminada is None:
            return None
        return round(self._t_terminada - self._t_iniciada, 4)

    def a_dict(self):
        return {
            'id': self.id,
            'tipo': self.tipo,
            'estado': self.estado.value,
            'parametros': self.parametros,
            'timeout': self.timeout,
            'resultado': self.resultado,
            'error': self.error,
            'worker': self.worker,
            'creada': _iso(self.creada),
            'iniciada': _iso(self.iniciada),
            'terminada': _iso(self.terminada),
            'espera_s': self.espera_s,
            'ejecucion_s': self.ejecucion_s,
        }


# --- Cuerpos del POST /tareas (validados por Pydantic, clase 19) ---

class ParamsDescargar(BaseModel):
    urls: list[HttpUrl] = Field(min_length=1, max_length=100)
    # Solo para la medición 1: descargar de a una con un for + await
    secuencial: bool = False


class ParamsHashear(BaseModel):
    texto: str
    iteraciones: int = Field(ge=1, le=100_000_000)


class ParamsEsperar(BaseModel):
    segundos: float = Field(gt=0, le=3600)


class _PedidoBase(BaseModel):
    timeout: float | None = Field(default=None, gt=0, le=3600)


class PedidoDescargar(_PedidoBase):
    tipo: Literal['descargar']
    parametros: ParamsDescargar


class PedidoHashear(_PedidoBase):
    tipo: Literal['hashear']
    parametros: ParamsHashear


class PedidoEsperar(_PedidoBase):
    tipo: Literal['esperar']
    parametros: ParamsEsperar


# El campo 'tipo' decide contra qué modelo se validan los parámetros
PedidoTarea = Annotated[
    Union[PedidoDescargar, PedidoHashear, PedidoEsperar],
    Field(discriminator='tipo'),
]


# --- Respuestas (response_model): FastAPI las valida y las documenta en /docs

class TareaCreada(BaseModel):
    """Lo que devuelve el POST: el id para consultarla y su estado inicial."""
    id: int
    estado: Estado


class TareaRespuesta(BaseModel):
    """Una tarea completa, tal como la ven GET y DELETE."""
    id: int
    tipo: str
    estado: Estado
    parametros: dict
    timeout: float
    resultado: Any = None
    error: str | None = None
    worker: int | None = None
    creada: str | None = None
    iniciada: str | None = None
    terminada: str | None = None
    espera_s: float | None = Field(
        default=None, description='Segundos en la cola antes de ejecutarse')
    ejecucion_s: float | None = Field(
        default=None, description='Segundos que tardó en ejecutarse')
