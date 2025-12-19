import azure.functions as func
import os
import json
from azure.cosmos import CosmosClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Variabili caricate da Azure Static Web App Settings
COSMOS_CONN_STR = os.environ.get("CosmosDBConnectionString")
DB_NAME = os.environ.get("DB_NAME")
CONTAINER_NAME = os.environ.get("CONTAINER_NAME")

# Inizializzazione client Cosmos
client = CosmosClient.from_connection_string(COSMOS_CONN_STR)
database = client.get_database_client(DB_NAME)
container = database.get_container_client(CONTAINER_NAME)

@app.route(route="GetDevices")
def get_devices(req: func.HttpRequest) -> func.HttpResponse:
    # Query per ottenere gli ID dei dispositivi senza duplicati
    query = "SELECT DISTINCT c.deviceId FROM c"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    devices = [item['deviceId'] for item in items]
    
    return func.HttpResponse(
        json.dumps(devices),
        mimetype="application/json",
        status_code=200
    )

@app.route(route="GetDeviceStats")
def get_device_stats(req: func.HttpRequest) -> func.HttpResponse:
    device_id = req.params.get('deviceId')
    
    if not device_id:
        return func.HttpResponse("Parametro deviceId mancante", status_code=400)

    # Query SQL
    query = """
    SELECT 
        AVG(c.temperature) as avgTemp, 
        AVG(c.humidity) as avgHum, 
        MIN(c.timestamp) as minTS, 
        MAX(c.timestamp) as maxTS 
    FROM c WHERE c.deviceId = @devId
    """
    params = [{"name": "@devId", "value": device_id}]
    
    items = list(container.query_items(query=query, parameters=params, enable_cross_partition_query=True))
    
    # Se il device esiste restituisce l'unico risultato
    result = items[0] if items else {}
    
    return func.HttpResponse(
        json.dumps(result),
        mimetype="application/json",
        status_code=200
    )