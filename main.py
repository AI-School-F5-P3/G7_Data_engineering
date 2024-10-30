from fastapi import FastAPI
from prometheus_client import start_http_server, Counter, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# Inicializamos la aplicación FastAPI
app = FastAPI()

# Creamos un contador de ejemplo para demostrar las métricas
request_counter = Counter('my_requests_total', 'Total number of requests to the root endpoint')

@app.get("/")
def read_root():
    # Incrementa el contador cada vez que accedes a la raíz
    request_counter.inc()
    return {"Hello": "World"}

# Agrega un endpoint para exponer métricas en /metrics
@app.get("/metrics")
def metrics():
    # Genera las métricas en el formato de Prometheus y devuelve la respuesta
    metrics_data = generate_latest()
    return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)
