#!/usr/bin/env python3
"""Servidor HTTP de prueba con demora configurable, para medir descargas.

Uso:
    python -m mediciones.servidor_prueba
    (en el compose es el servicio 'prueba'; desde la api: http://prueba:8001)

    GET /demora?seg=0.5&kb=20     espera 0.5 s y devuelve 20 KB

Simula un servidor remoto lento sin depender de internet. La espera es
await asyncio.sleep: cede el control, así que atiende muchos pedidos a la
vez en un solo hilo (clase 18).
"""
import asyncio

from fastapi import FastAPI, Query, Response

PUERTO = 8001

app = FastAPI(title='Servidor de prueba',
              description='Responde después de una demora controlada')


@app.get('/demora')
async def demora(
    seg: float = Query(default=0, ge=0, le=60, description='Segundos de espera'),
    kb: int = Query(default=1, ge=0, le=10_240, description='Tamaño en KB'),
):
    """Espera `seg` segundos y devuelve `kb` kilobytes."""
    await asyncio.sleep(seg)
    return Response(b'x' * (kb * 1024), media_type='application/octet-stream')


if __name__ == '__main__':
    import uvicorn
    # Sin log de acceso: un renglón por pedido ensucia las mediciones
    uvicorn.run(app, host='0.0.0.0', port=PUERTO, access_log=False)
