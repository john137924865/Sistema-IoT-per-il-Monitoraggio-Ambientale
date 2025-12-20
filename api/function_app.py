import azure.functions as func
import os
import json
from azure.cosmos import CosmosClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_container():
    conn_str = os.environ.get("CosmosDBConnectionString")
    db_name = os.environ.get("DB_NAME")
    container_name = os.environ.get("DB_CONTAINER_NAME")
    
    if not all([conn_str, db_name, container_name]):
        raise ValueError("Mancano variabili d'ambiente!")
        
    client = CosmosClient.from_connection_string(conn_str)
    return client.get_database_client(db_name).get_container_client(container_name)

@app.route(route="GetDevices", methods=["GET"])
def get_devices(req: func.HttpRequest) -> func.HttpResponse:
    try:
        container = get_container()
        query = "SELECT DISTINCT c.deviceId FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        devices = [item['deviceId'] for item in items]
        return func.HttpResponse(json.dumps(devices), mimetype="application/json", status_code=200)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")

@app.route(route="GetDeviceStats", methods=["GET"])
def get_device_stats(req: func.HttpRequest) -> func.HttpResponse:
    try:
        device_id = req.params.get('deviceId')
        if not device_id:
            return func.HttpResponse("Parametro deviceId mancante", status_code=400)

        container = get_container()

        query = """
        SELECT 
            AVG(c.temperature) as avgTemp, 
            AVG(c.humidity) as avgHum, 
            COUNT(1) as cnt 
        FROM c 
        WHERE c.deviceId = @devId
        """
        params = [{"name": "@devId", "value": device_id}]
        
        items = list(container.query_items(query=query, parameters=params, enable_cross_partition_query=True))

        if not items or items[0].get('cnt') == 0:
            return func.HttpResponse(
                json.dumps({"error": "Nessun dato trovato per questo ID", "id_cercato": device_id}), 
                status_code=404, 
                mimetype="application/json"
            )

        stats = items[0]
        temp = stats.get('avgTemp')
        hum = stats.get('avgHum')

        response_data = {
            "deviceId": device_id,
            "avgTemp": round(float(temp), 2) if temp is not None else 0,
            "avgHum": round(float(hum), 2) if hum is not None else 0,
            "count": stats.get('cnt')
        }

        return func.HttpResponse(
            json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"Dettaglio_Errore_Python": str(e)}), 
            status_code=500, 
            mimetype="application/json"
        )