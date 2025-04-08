import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Configuración de MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TOPIC_FLUJO = os.getenv("MQTT_TOPIC_FLUJO")
MQTT_TOPIC_VOLUMEN = os.getenv("MQTT_TOPIC_VOLUMEN")
MQTT_TOPIC_VALVULA_APERTURA = os.getenv("MQTT_TOPIC_VALVULA_APERTURA")
MQTT_TOPIC_CONSIGNA = os.getenv("MQTT_TOPIC_CONSIGNA")

# Configuración de InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Conexión a InfluxDB
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN)
write_api = influx_client.write_api(write_options=WritePrecision.NS)

# Función para manejar el mensaje recibido en MQTT
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Received message: {payload} on topic: {msg.topic}")

    # Publicar datos en InfluxDB
    if msg.topic == MQTT_TOPIC_FLUJO:
        flujo_data = float(payload)
        point = Point("flujo").tag("sensor", "flujo_agua").field("value", flujo_data)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    elif msg.topic == MQTT_TOPIC_VOLUMEN:
        volumen_data = float(payload)
        point = Point("volumen").tag("sensor", "volumen_agua").field("value", volumen_data)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    elif msg.topic == MQTT_TOPIC_VALVULA_APERTURA:
        apertura_data = float(payload)
        point = Point("valvula_apertura").tag("sensor", "apertura_valvula").field("value", apertura_data)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    elif msg.topic == MQTT_TOPIC_CONSIGNA:
        consigna_data = float(payload)
        point = Point("valvula_consigna").tag("sensor", "consigna_valvula").field("value", consigna_data)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

# Conexión a MQTT
client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.connect(MQTT_BROKER, MQTT_PORT)

# Suscribirse a los topics
client.subscribe(MQTT_TOPIC_FLUJO)
client.subscribe(MQTT_TOPIC_VOLUMEN)
client.subscribe(MQTT_TOPIC_VALVULA_APERTURA)
client.subscribe(MQTT_TOPIC_CONSIGNA)

# Establecer la función que maneja los mensajes
client.on_message = on_message

# Mantener la conexión abierta
client.loop_forever()
