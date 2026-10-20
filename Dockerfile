FROM python:3.12-slim

# Sin .pyc en la imagen y con la salida sin buffer, para ver los logs al instante
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Las dependencias primero: cambian poco, y así Docker reutiliza esta capa
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/
COPY mediciones/ mediciones/

EXPOSE 8000

# Forma exec (lista): python es el PID 1 y recibe el SIGTERM de docker stop
# directamente, sin un shell en el medio que se lo trague
CMD ["python", "-m", "app.main"]
