"""Configuración leída de variables de entorno (las define docker-compose.yml)."""
import os
from dataclasses import dataclass


def _entero(nombre, defecto):
    # Una variable definida pero vacía (PROCESOS= en compose) usa el defecto
    return int(os.environ.get(nombre) or defecto)


def _decimal(nombre, defecto):
    return float(os.environ.get(nombre) or defecto)


@dataclass(frozen=True)
class Config:
    # Cantidad de corrutinas worker que consumen de la cola
    workers: int = _entero('WORKERS', 4)
    # Capacidad de la cola: si se llena, POST /tareas responde 503 (backpressure)
    cola_max: int = _entero('COLA_MAX', 1000)
    # Procesos del ProcessPoolExecutor para las tareas CPU-bound
    procesos: int = _entero('PROCESOS', os.cpu_count() or 2)
    # Timeout por defecto de cada tarea, en segundos
    timeout_defecto: float = _decimal('TIMEOUT_DEFECTO', 60)
    # Cuánto se espera a las tareas en curso al apagar, antes de cancelarlas
    limite_apagado: float = _decimal('LIMITE_APAGADO', 20)
    # Dónde ejecutar hashear: 'proceso' (correcto), 'hilo' o 'directo' (bloquea
    # el event loop a propósito, solo para la medición 2)
    hash_modo: str = os.environ.get('HASH_MODO', 'proceso')
    # Iteraciones de SHA-256 por cada viaje al executor
    hash_bloque: int = _entero('HASH_BLOQUE', 200_000)
    # Timeout de cada descarga individual
    timeout_descarga: float = _decimal('TIMEOUT_DESCARGA', 15)
    # Conexiones HTTP simultáneas como máximo, entre todas las descargas
    conexiones_max: int = _entero('CONEXIONES_MAX', 100)
    dir_volcados: str = os.environ.get('DIR_VOLCADOS', 'volcados')
    puerto: int = _entero('PUERTO', 8000)


config = Config()

if config.hash_modo not in ('proceso', 'hilo', 'directo'):
    raise ValueError(f'HASH_MODO inválido: {config.hash_modo!r}')
