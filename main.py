import os
import paho.mqtt.client as mqtt
from influxdb_client_3 import InfluxDBClient3, Point
from dotenv import load_dotenv
import json
import re
import time
from datetime import datetime, timedelta, timezone

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# Configuración InfluxDB 3.0
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET") 
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG") 

# Crear cliente InfluxDB
influx_client = InfluxDBClient3(
    host=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)

# Callback al conectar al broker MQTT
def on_connect(client, userdata, flags, rc):
    print(f"on_connect: {rc}")
    if rc == 0:
        print("✅ Conexión exitosa al broker MQTT.")
        client.subscribe("contador/#")
        client.subscribe("valvula/#")
        print("📡 Suscripción a 'contador/#' y 'valvula/#'")
    else:
        print(f"❌ Conexión fallida. Código: {rc}")

# Callback al recibir mensaje MQTT
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"📩 Mensaje recibido: {payload} en el topic: {topic}")

    try:
        # Parsear el JSON
        payload_json = json.loads(payload)
        data = float(payload_json["data"])
        timestamp = int(payload_json["timestamp"])

        # Expresión regular para capturar tipo, id y medida
        match = re.match(r'^(contador|valvula)/(\d{2})/(flujo|volumen|apertura)$', topic)
        if not match:
            print(f"⚠️ Topic no válido: {topic}")
            return

        tipo, id_tag, medicion = match.groups()

        # Nombre de la medida en InfluxDB
        if medicion == "apertura":
            point_name = "valvula_apertura"
        else:
            point_name = medicion  # "flujo" o "volumen"
        
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        print(f"📩 El Point es: {point_name}, con el tag: {tipo} {id_tag}, en el timestamp {dt} ({timestamp}) con el valor {data}")
        print(f"La hora a lo pampa {datetime.now(timezone.utc)}")

        # Crear el punto
        point = (
            Point(point_name)
            .tag(tipo, id_tag)
            .field("value", data)
            .time(dt)
        )

        # Guardar en InfluxDB
        influx_client._write_api.write(
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
            record=point
        )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"❌ Error al procesar el mensaje: {e}. Payload: {payload}")

# Configuración del cliente MQTT
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(ca_certs="ca-cert.pem")  # TLS si es necesario

# Conectar
try:
    print(f"🔌 Conectando al broker MQTT en {MQTT_BROKER}:{MQTT_PORT}...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
except Exception as e:
    print(f"❌ Error de conexión MQTT: {e}")

# Asignar callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Bucle principal
print("🚀 Iniciando bucle MQTT...")
mqtt_client.loop_forever()