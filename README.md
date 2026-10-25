# TP2 - Plataforma de Tareas Distribuidas

Computación II - Universidad de Mendoza - 2026
Nicolás Almenara

## 1. Descripción y uso

Es un servicio HTTP al que se le mandan tareas para que las haga en segundo
plano. El cliente recibe un id apenas manda la tarea y después puede
consultar cómo va, ver el resultado o cancelarla. Por dentro hay una cola
(`asyncio.Queue`) y un grupo de workers que van sacando tareas de ahí. Está
hecho con FastAPI y asyncio, y las tareas que usan mucho CPU se mandan a un
`ProcessPoolExecutor`.

Hay tres tipos de tarea:

- `descargar`: baja una lista de URLs, todas a la vez (o de a una si se pide
  `"secuencial": true`, que uso para comparar).
- `hashear`: aplica SHA-256 a un texto la cantidad de veces que se pida.
- `esperar`: duerme N segundos. Sirve para probar la cola, los timeouts y las
  cancelaciones.

### Cómo levantarlo

```bash
docker compose up --build
```

La API queda en http://localhost:8000 y en http://localhost:8000/docs está la
documentación que genera FastAPI, desde donde también se puede probar todo.
El compose levanta además un servicio `prueba`, un servidor que responde
después de la demora que se le pida (`http://prueba:8001/demora?seg=1`). Lo
usé para medir descargas sin depender de internet.

Para apagarlo alcanza con `Ctrl+C` o `docker compose down`. El servidor
termina en orden y guarda el estado de todas las tareas en
`volcados/estado_<fecha>.json`.

Algunas cosas se pueden cambiar con variables de entorno al levantar, por
ejemplo `WORKERS=8 docker compose up`. Las principales son `WORKERS`
(cantidad de workers, 4 por defecto), `COLA_MAX` (tamaño de la cola, 1000),
`PROCESOS` (procesos para el hash, por defecto la cantidad de núcleos),
`TIMEOUT_DEFECTO` (60 s) y `LIMITE_APAGADO` (cuánto se espera a las tareas
al apagar, 20 s). `HASH_MODO` existe solo para la medición 2.

### Endpoints

- `POST /tareas`: crea una tarea. Responde 202 con el id. Si los datos están
  mal responde 422, y si la cola está llena o el servidor se está apagando,
  503.
- `GET /tareas`: lista todas. Se puede filtrar con `?estado=ejecutando`.
- `GET /tareas/{id}`: estado y resultado de una tarea, o 404 si no existe.
- `DELETE /tareas/{id}`: cancela una tarea pendiente o en ejecución. Si ya
  terminó responde 409.
- `GET /estadisticas`: cuántas hay en cada estado, tiempos promedio, workers
  ocupados y lugares en la cola.

Los estados posibles son `pendiente`, `ejecutando`, `completada`, `fallida` y
`cancelada`.

### Ejemplos con curl

```bash
J='Content-Type: application/json'

curl -X POST localhost:8000/tareas -H "$J" \
     -d '{"tipo": "esperar", "parametros": {"segundos": 10}}'
# {"id":1,"estado":"pendiente"}

curl -X POST localhost:8000/tareas -H "$J" \
     -d '{"tipo": "hashear", "parametros": {"texto": "hola", "iteraciones": 5000000}}'

curl -X POST localhost:8000/tareas -H "$J" \
     -d '{"tipo": "descargar", "parametros": {"urls": ["https://example.com",
          "http://prueba:8001/demora?seg=1&kb=100"]}}'

# con timeout propio: a los 2 s queda fallida
curl -X POST localhost:8000/tareas -H "$J" \
     -d '{"tipo": "esperar", "parametros": {"segundos": 30}, "timeout": 2}'

curl localhost:8000/tareas
curl localhost:8000/tareas/1
curl -X DELETE localhost:8000/tareas/1
curl localhost:8000/estadisticas
```

Ojo que sin el header `Content-Type: application/json` el POST devuelve 422.

Para ver el apagado ordenado con tareas corriendo:

```bash
for s in 5 50 50; do
  curl -s -X POST localhost:8000/tareas -H "$J" \
       -d "{\"tipo\":\"esperar\",\"parametros\":{\"segundos\":$s}}"; done
docker compose kill -s SIGTERM api
docker compose logs -f api
```

La de 5 segundos termina bien y las de 50 se cancelan cuando se cumplen los
20 segundos de límite. Al volver a levantar, el log dice qué había en el
último volcado.

### Archivos

En `app/` está la plataforma: `main.py` tiene la API y el manejo de señales,
`plataforma.py` la cola, los workers, la cancelación y el apagado,
`tareas.py` los tres tipos de tarea, `modelos.py` los estados y la
validación, `persistencia.py` el volcado a JSON y `config.py` las variables
de entorno. En `mediciones/` están los tres scripts de medición y el
servidor de prueba.

## 2. Arquitectura

Todo corre en un solo proceso con un event loop. Lo único que sale de ahí es
el cálculo del hash, que va a un pool de procesos aparte.

```
 POST /tareas --> encolar() --(cola llena)--> 503
                     |
                     v
              asyncio.Queue
                     |
       +-------------+-------------+
       v             v             v
   worker 0      worker 1  ...  worker N
       |
       |  cada tarea corre en su propia Task, con timeout
       v
   descargar --> httpx, todas las URLs a la vez
   esperar   --> asyncio.sleep
   hashear   --> ProcessPoolExecutor (otro proceso)

 GET    --> lee el diccionario de tareas
 DELETE --> cancela la Task de esa tarea
 SIGTERM / SIGINT --> apagado ordenado y volcado a JSON
```

El recorrido de una tarea es así. Cuando llega el POST, Pydantic valida los
datos según el `tipo`. Si están bien, se crea la tarea en estado `pendiente`,
se guarda en un diccionario, se mete en la cola y se responde 202 sin esperar
nada más. Un worker libre la saca de la cola, la pasa a `ejecutando` y la
corre en una Task propia con su timeout. Cuando termina queda `completada`,
`fallida` o `cancelada`, y el worker vuelve a buscar otra a la cola. Los GET
solo leen el diccionario, así que se pueden hacer en cualquier momento.

## 3. Decisiones de diseño

### ¿Por qué asyncio.Queue y no queue.Queue?

Porque `queue.Queue` está pensada para threads. Su `get()` bloquea el hilo
que lo llama, y acá hay un solo hilo, el del event loop. Si un worker se
quedara esperando ahí, se congelaría el servidor entero, incluidas las
respuestas HTTP. En cambio `asyncio.Queue.get()` es una corrutina: si la cola
está vacía el worker queda suspendido y el loop sigue atendiendo otras cosas.
Tampoco necesita lock porque todos los que la usan están en el mismo hilo. Es
el mismo productor-consumidor de la clase 11 pero con corrutinas.

Para meter tareas uso `put_nowait()` en vez de `await put()`. Si la cola está
llena prefiero contestar 503 enseguida antes que dejar al cliente esperando
sin saber qué pasa.

### ¿Cómo evito que una tarea CPU-bound congele la API?

`hashear` no se calcula en el event loop sino en un `ProcessPoolExecutor`,
usando `run_in_executor`. Mientras el otro proceso calcula, la corrutina
espera en un `await` y el loop sigue libre. En la medición 2 se ve la
diferencia: sin executor un GET tardó 9.8 segundos, con executor 3.5 ms.

Usé procesos y no threads porque con un thread el loop no se congela pero
igual compite por el GIL con el cálculo. Lo medí y con un thread cada GET
tarda unos 13 ms, casi cuatro veces más. Además, varios hashes en threads no
corren en paralelo.

El hash se manda al executor en bloques de 200.000 iteraciones (unos 100 ms
cada uno). Lo hice así por la cancelación: si fuera una sola llamada, al
cancelar se cortaría la espera pero el otro proceso seguiría calculando
hasta el final. Con bloques, termina el bloque actual y no se manda el
siguiente.

El pool se crea una sola vez al arrancar y usa `spawn`. En la clase 8 vimos
que hacer `fork()` en un proceso que tiene threads puede dejar un lock
heredado tomado, y este proceso usa threads para escribir el JSON. Por usar
`spawn` el servidor arranca dentro de `if __name__ == '__main__'`, porque
cada proceso hijo vuelve a importar el módulo.

Otras cosas que podrían bloquear el loop también las saqué de ahí: la
escritura del JSON va con `asyncio.to_thread`, las descargas usan
`httpx.AsyncClient` y las esperas `asyncio.sleep`.

### ¿Cómo implementé la cancelación, y qué pasa con los recursos a medio usar?

Cada tarea corre en su propia Task, separada del worker. El `DELETE` llama a
`cancel()` sobre esa Task, y eso hace que en el próximo `await` de la tarea
salte un `CancelledError`. La tarea lo captura, se marca como `cancelada` y lo
vuelve a lanzar (si no lo relanzara, para asyncio la Task habría terminado
normal). El worker espera a la tarea con `asyncio.wait`, que no le pasa la
cancelación, así que el worker sigue vivo y puede tomar otra tarea.

Con los recursos pasa lo siguiente según el tipo:

- En `esperar` se corta el `sleep` y no queda nada.
- En `descargar` las descargas están dentro de un `TaskGroup`. Cuando se
  cancela, el grupo cancela a todas las descargas y espera a que terminen,
  así que no quedan corrutinas sueltas. Además cada una usa
  `async with cliente.stream(...)`, que cierra la conexión aunque la
  cancelación llegue a mitad de la lectura.
- En `hashear` el bloque que se estaba calculando termina, se descarta y no
  se mandan más.
- Si la tarea todavía estaba en la cola, como no se puede sacar algo del
  medio de una `asyncio.Queue`, la marco como `cancelada` y el worker que la
  saque la saltea.

Cuando probé cancelar un hash y una descarga a mitad de camino, el DELETE
respondió en 3 o 4 ms con la tarea ya cancelada.

El timeout funciona con el mismo mecanismo. `asyncio.timeout` cancela la
tarea cuando se pasa del tiempo, pero convierte el `CancelledError` en
`TimeoutError`. Así puedo diferenciar los dos casos: si fue timeout la tarea
queda `fallida` con el motivo, y si fue un DELETE queda `cancelada`.

### ¿Qué pasa si un worker lanza una excepción no capturada?

Si el error es de la tarea (una URL que no existe, un timeout, lo que sea),
lo captura la misma tarea y queda `fallida` con el mensaje. El worker no se
entera.

Si el error es del worker en sí, el worker marca como `fallida` la tarea que
estaba haciendo, para que no quede colgada en `ejecutando`, y se muere. Cada
worker tiene un `add_done_callback` que se da cuenta de eso y lanza otro
worker en su lugar, parecido al supervisor del ejercicio 6 de la clase 6. Sin
esto cada bug iría matando workers sin que nadie se entere, y en algún
momento la API seguiría aceptando tareas que nadie ejecuta. El
`cola.task_done()` está en un `finally` para que la cuenta de la cola quede
bien aunque el worker muera.

### ¿Por qué no hace falta un Lock?

Porque todo corre en un solo hilo y el loop solo cambia de corrutina en un
`await`. Si entre leer y modificar algo no hay ningún `await`, nadie más
puede meterse en el medio. Lo tuve en cuenta en el contador de workers
ocupados, en `encolar()` y en el apagado. Con threads (clase 11) el cambio
puede pasar en cualquier instrucción; acá se ve en el código dónde puede
pasar.

### Apagado ordenado

uvicorn trae sus propios manejadores de SIGTERM y SIGINT que cierran el
servidor enseguida. Para controlar el orden lo arranco con `uvicorn.Server`
y registro mis manejadores con `loop.add_signal_handler`, como vimos en la
clase 20. Cuando llega la señal pasa esto:

1. Los POST empiezan a recibir 503, pero los GET siguen funcionando.
2. Las tareas pendientes se cancelan, porque todavía no empezaron.
3. Se mete en la cola un centinela por cada worker. Cada worker termina lo
   que estaba haciendo, toma el centinela y sale.
4. Si después de 20 segundos alguna tarea sigue corriendo, se cancela.
5. Recién ahí se cierra uvicorn, se guarda el JSON y se cierran el cliente
   HTTP y el pool de procesos.

En el compose puse `stop_grace_period: 30s` para que Docker no mate el
proceso con SIGKILL antes de que termine, y el `CMD` del Dockerfile está en
forma exec para que Python sea el PID 1 y le llegue la señal directo.

### Otras decisiones

Uso un solo `httpx.AsyncClient` para todas las descargas, así se reutilizan
las conexiones. Tiene timeout y un máximo de 100 conexiones abiertas, que
cumple el mismo papel que el `Semaphore` de la clase 20.

El POST responde 202 y no 201 porque la tarea no está hecha todavía, solo
quedó aceptada para hacerla después.

Para los tiempos de espera y ejecución uso `time.monotonic()`, que no se ve
afectado si cambia la hora del sistema.

## 4. Mediciones

Las hice en mi notebook (Ryzen 7 7730U, 8 núcleos), con Docker. Estaba a
batería, así que los tiempos son algo más lentos que enchufada, pero repetí
las mediciones y las proporciones se mantienen. Los scripts están en
`mediciones/` y se corren dentro del contenedor, por ejemplo
`docker compose exec api python -m mediciones.medir_descargas`.

### Medición 1: concurrencia I/O

Mandé dos tareas `descargar` con las mismas 20 URLs, una en modo secuencial
(un `for` con `await` adentro) y otra concurrente (con `TaskGroup`).

Contra el servidor de prueba, con demoras al azar que suman 11.45 segundos y
la más lenta de 1.15 s:

```
secuencial    11.57 s
concurrente    1.20 s     (9.6 veces más rápido)
```

Contra 20 sitios reales de internet, promedio de 4 repeticiones:

```
secuencial     6.61 s
concurrente    1.30 s     (5.1 veces más rápido)
```

Con el servidor local se ve justo lo que se esperaba: la secuencial tarda la
suma de todas las demoras y la concurrente lo que tarda la más lenta. Mientras
una descarga espera respuesta, el loop atiende a las demás, así que las 20
esperas se superponen. No llega a 20 veces porque el máximo posible es la
suma dividida la más lenta, unas 10 veces.

Con internet la mejora es menor porque la mayoría de los sitios responde en
0.1 a 0.3 segundos y hay poco que superponer. También varía mucho: la primera
vez siempre tarda más porque tiene que resolver DNS y hacer el handshake TLS.

### Medición 2: event loop bloqueado

Lancé un `hashear` de 15 millones de iteraciones (unos 9.5 segundos de CPU) y
mientras corría hice `GET /tareas` uno atrás de otro. Sin nada corriendo, el
GET tarda unos 4.4 ms.

```
sin executor (hash en la corrutina)    1 GET     9790 ms
con ProcessPoolExecutor              177 GETs     3.5 ms promedio
con un thread (asyncio.to_thread)    156 GETs    12.9 ms promedio
```

Sin executor, el único GET que se pudo hacer tardó lo mismo que todo el hash.
El pedido llegó, pero el loop estaba ocupado calculando y no volvió a mirar
los sockets hasta terminar. Durante esos 10 segundos el servidor no atendió
nada. Probé además mandar el hash con `timeout: 1`: sin executor terminó
`completada` a los 9.7 segundos, porque el timer del timeout nunca pudo
correr. Con executor quedó `fallida` justo al segundo.

Que haya un solo GET también es un resultado. El script espera cada respuesta
antes de mandar la siguiente, así que mientras el servidor está trabado no
manda nada y esos pedidos no aparecen en la medición. Es lo que en la clase 14
se llamó coordinated omission: en la realidad todos los clientes que llegaran
en esos 10 segundos habrían tenido que esperar.

Con el executor el hash tarda lo mismo, porque el cálculo no se hace más
rápido, pero el servidor sigue respondiendo normal.

### Medición 3: techo del pool

Mandé K tareas iguales de golpe y medí cuánto tardaba en terminar la última y
cuántas tareas por segundo se completaban. Los POST los mando desde 8
procesos, porque al principio lo hacía desde uno solo y terminé midiendo el
límite del script en vez del servidor.

Con tareas `esperar` de 0.5 s y 4 workers, el límite son los workers:

```
K = 4     0.65 s    6.2 tareas/s
K = 16    2.10 s    7.6 tareas/s
K = 64    8.30 s    7.7 tareas/s
```

Cada tarea sigue tardando 0.5 s en ejecutarse; lo que crece es lo que espera
en la cola. Más de 8 tareas por segundo (4 workers / 0.5 s) no se puede.

Con `hashear` de un millón de iteraciones y 64 workers, el límite son los
núcleos:

```
K = 8     0.86 s     9.3 tareas/s
K = 16    1.53 s    10.4 tareas/s
K = 32    2.89 s    11.1 tareas/s
```

Hasta 8 tareas cada una tiene su núcleo y tardan lo mismo que una sola.
Después se empiezan a hacer fila en el executor. Poner más workers no cambia
nada porque el problema es el CPU.

Con tareas `esperar` y 1000 workers, el que se satura es el event loop:

```
K = 100       0.78 s     128 tareas/s
K = 1000      1.41 s     711 tareas/s
K = 5000      4.78 s    1045 tareas/s
```

Mil corrutinas esperando no le cuestan casi nada al servidor, pero cada POST
hay que parsearlo, validarlo y responderlo, y todo eso es CPU en el único
hilo del loop. Mientras llegaban los pedidos la API estaba al 100% de un
núcleo. Se nota también en que las tareas de 0.5 s empiezan a tardar 0.58 s,
porque el loop atiende tarde los timers.

Con K = 10.000 los números variaron bastante: hice cuatro corridas, tardaron
entre 7.4 y 12.4 segundos, y en dos hubo rechazos (433 y 350 tareas con 503)
y en las otras dos ninguno. Los rechazos aparecen cuando entran POST más
rápido de lo que los workers vacían la cola, y a batería eso depende de a qué
velocidad ande el CPU en ese momento.

Por último, con la cola limitada a 100 lugares, 4 workers y 500 tareas de
golpe, se aceptaron 104 y se rechazaron 396 con 503.

Entonces, a la pregunta de qué se rompe primero si llegan 10.000 tareas: lo
primero que llega al límite es la cola, y está hecho a propósito. Es mejor
contestar 503 que aceptar trabajo que no se va a poder hacer y que la memoria
crezca sin límite. Con pocos workers el límite son los workers, con tareas de
CPU son los núcleos, y con muchos workers es el propio event loop.

## 5. Conceptos del curso aplicados

Los workers consumen de una `asyncio.Queue` compartida, que es el patrón
productor-consumidor de la clase 11 pero con corrutinas. No hace falta lock
porque el loop no cambia de corrutina entre `await`s. La cola tiene tamaño
máximo y si se llena se responde 503, que es backpressure (clases 12 y 21).
Para apagar los workers uso un centinela por worker, como en la clase 21.

La medición 2 es la clase 18 en la práctica: asyncio da concurrencia pero no
paralelismo, y una corrutina que no cede congela todo. Por eso `hashear` va a
un `ProcessPoolExecutor` (clases 9 y 21), y la comparación con threads
muestra el efecto del GIL de la clase 10. El pool usa `spawn` por lo que
vimos de fork en la clase 8.

La cancelación es como la explica la clase 20: `cancel()` no frena nada en el
momento, inyecta un `CancelledError` en el próximo `await`, y hay que
relanzarlo siempre. El timeout es un caso particular de cancelación. Las
descargas usan `TaskGroup` (clase 21) para que no queden tareas huérfanas, y
la diferencia entre el `for` con `await` y el `TaskGroup` es la medición 1.

Las señales se manejan con `loop.add_signal_handler` (clase 20), que es más
seguro que `signal.signal()` de la clase 6 porque el handler corre entre
tareas. El callback que repone workers caídos es la misma idea del
supervisor de la clase 6. El `CMD` en forma exec es para que el PID 1 del
contenedor reciba las señales (clases 1 y 6).

La API sigue lo de la clase 19: FastAPI, validación con Pydantic y códigos
HTTP (4xx cuando el error es del cliente y 5xx cuando es del servidor). El
cliente httpx reutiliza conexiones con keep-alive y limita las conexiones
abiertas como el `Semaphore` de la clase 20. Que 1000 workers sean 1000
corrutinas y no 1000 threads es la respuesta al problema C10K (clases 14 y
17). El JSON se escribe con `asyncio.to_thread` porque asyncio no hace I/O de
archivos asíncrono, y se escribe primero a un temporal y después se renombra
con `os.replace`, que es atómico.

## 6. Limitaciones conocidas

- Todo está en memoria y en un solo proceso. No se puede escalar con varios
  procesos de uvicorn porque cada uno tendría su propia cola. Además las
  tareas terminadas nunca se borran.
- El volcado se guarda al apagar pero no se vuelve a cargar al arrancar,
  solo se informa en el log.
- `GET /tareas` devuelve todo junto. Con decenas de miles de tareas armar esa
  respuesta es trabajo pesado en el loop y frena a los demás.
- El techo de la API es un núcleo: el trabajo pesado sale del loop, pero
  atender HTTP no.
- Cancelar un `hashear` puede tardar hasta un bloque, unos 100 ms.
- Las tareas pendientes que se cancelan siguen ocupando lugar en la cola
  hasta que un worker las saca.
- `descargar` acepta cualquier URL, incluso direcciones internas de la red de
  Docker. Si estuviera expuesto a internet sería un problema de seguridad.
- `loop.add_signal_handler` no existe en Windows, así que ahí hay que usar
  Docker.

## 7. Lo que aprendí

Lo que más me costó entender fue que en asyncio nada se interrumpe solo. Lo
tenía claro en la teoría, pero lo vi de verdad en la medición 2: con el hash
corriendo en la corrutina, un GET tardó casi 10 segundos, y una tarea con
timeout de 1 segundo terminó como completada porque el timer del timeout
nunca llegó a correr. Ahí entendí por qué la clase insiste con que una
corrutina que no cede bloquea todo, y que con `cancel()` pasa lo mismo: no
mata nada, mete una excepción en el próximo `await`. Por eso tuve que partir
el hash en bloques, si no la cancelación no servía para nada.

También aprendí a desconfiar de mis mediciones. La primera versión de la
medición 3 me daba un techo de unos 400 pedidos por segundo y parecía
razonable, hasta que miré el uso de CPU y el que estaba al 100% era el script
que mandaba los pedidos, no el servidor. Algo parecido me pasó con las 10.000
tareas: la primera vez hubo rechazos y la siguiente no, así que no podía
presentar un solo número como si fuera la respuesta.

Por último, el apagado ordenado me pareció mucho más difícil de lo que
esperaba. Dejar de aceptar tareas, esperar a las que están corriendo pero no
para siempre, y guardar todo antes de salir parece simple, pero hay que
pensar qué pasa con cada tarea según en qué estado está. Me sirvió juntar
cosas de clases distintas (señales, colas, Docker) en un mismo programa y ver
que encajan.
