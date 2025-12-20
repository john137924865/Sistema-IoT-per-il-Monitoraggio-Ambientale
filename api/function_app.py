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
            return func.HttpResponse("Manca deviceId", status_code=400)

        container = get_container()
        
        query = """
            SELECT 
                c.deviceId,
                AVG(c.temperature) as avgTemp, 
                AVG(c.humidity) as avgHum, 
                MIN(c.timestamp) as minTS, 
                MAX(c.timestamp) as maxTS,
                COUNT(1) as cnt
            FROM c 
            WHERE c.deviceId = @devId
            GROUP BY c.deviceId
        """
        
        params = [{"name": "@devId", "value": device_id}]
        
        items = list(container.query_items(
            query=query, 
            parameters=params, 
            partition_key=device_id
        ))


        if not items or items[0].get('cnt') == 0:
            return func.HttpResponse(json.dumps({
                "avgTemp": 0, "avgHum": 0, "minTS": "N/A", "maxTS": "N/A"
            }), mimetype="application/json")

        res = items[0]
        
        result = {
            "avgTemp": float(res.get('avgTemp') or 0),
            "avgHum": float(res.get('avgHum') or 0),
            "minTS": str(res.get('minTS') or "N/A"),
            "maxTS": str(res.get('maxTS') or "N/A"),
            "count": int(res.get('cnt') or 0)
        }
        
        return func.HttpResponse(json.dumps(result), mimetype="application/json")

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)
    

@app.route(route="GetDeviceHistory", methods=["GET"])
def get_device_history(req: func.HttpRequest) -> func.HttpResponse:
    try:
        device_id = req.params.get('deviceId')
        if not device_id:
            return func.HttpResponse("Manca deviceId", status_code=400)

        container = get_container()

        query = "SELECT c.timestamp, c.temperature, c.humidity FROM c WHERE c.deviceId = @devId ORDER BY c.timestamp ASC"
        params = [{"name": "@devId", "value": device_id}]
        
        items = list(container.query_items(query=query, parameters=params, enable_cross_partition_query=True))
        
        return func.HttpResponse(json.dumps(items), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)