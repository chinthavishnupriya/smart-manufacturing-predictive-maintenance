"""
MQTT -> Kafka Bridge

Architecture:

IoT Sensor
    ↓
HiveMQ MQTT Broker
    ↓
This bridge
    ↓
Kafka sensor_raw topic
    ↓
Spark Structured Streaming
"""

import json
import socket
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883

MQTT_TOPIC = "smart-manufacturing/sensors"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sensor_raw"

MQTT_CLIENT_ID = (
    f"smart-manufacturing-bridge-"
    f"{socket.gethostname()}"
)


# ============================================================
# KAFKA PRODUCER
# ============================================================

kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),

    acks="all",

    retries=5,
)


# ============================================================
# MQTT CALLBACK: CONNECT
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties=None,
):

    print(
        f"[MQTT] Connected to "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    print(
        f"[MQTT] Subscribing to: "
        f"{MQTT_TOPIC}"
    )

    result, _ = client.subscribe(
        MQTT_TOPIC,
        qos=1,
    )

    if result == mqtt.MQTT_ERR_SUCCESS:

        print(
            "[MQTT] Subscription successful"
        )

    else:

        print(
            f"[MQTT] Subscription failed: "
            f"{result}"
        )


# ============================================================
# MQTT CALLBACK: MESSAGE
# ============================================================

def on_message(
    client,
    userdata,
    message,
):

    try:

        payload = message.payload.decode(
            "utf-8"
        )

        data = json.loads(payload)


        # ----------------------------------------------------
        # Validate required fields
        # ----------------------------------------------------

        required_fields = [
            "machine_id",
            "timestamp",
            "temperature",
            "vibration",
            "rpm",
            "pressure",
            "operating_hours",
            "failure",
        ]


        missing = [
            field
            for field in required_fields
            if field not in data
        ]


        if missing:

            print(
                "[ERROR] Missing fields: "
                f"{missing}"
            )

            return


        # ----------------------------------------------------
        # Send to Kafka
        # ----------------------------------------------------

        future = kafka_producer.send(
            KAFKA_TOPIC,
            value=data,
        )

        future.get(
            timeout=10
        )

        kafka_producer.flush()


        print(
            "[MQTT → KAFKA] "
            f"machine={data['machine_id']} | "
            f"temp={data['temperature']} | "
            f"vibration={data['vibration']} | "
            f"rpm={data['rpm']} | "
            f"pressure={data['pressure']} | "
            f"failure={data['failure']}"
        )


    except json.JSONDecodeError:

        print(
            "[ERROR] MQTT message is not valid JSON:"
        )

        print(
            message.payload.decode(
                "utf-8",
                errors="replace",
            )
        )


    except Exception as e:

        print(
            f"[ERROR] MQTT → Kafka failed: {e}"
        )


# ============================================================
# MQTT CALLBACK: DISCONNECT
# ============================================================

def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties=None,
):

    print(
        f"[MQTT] Disconnected: "
        f"{reason_code}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)

    print(
        "SMART MANUFACTURING MQTT → KAFKA BRIDGE"
    )

    print("=" * 60)

    print(
        f"MQTT broker : "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    print(
        f"MQTT topic  : "
        f"{MQTT_TOPIC}"
    )

    print(
        f"Kafka       : "
        f"{KAFKA_BOOTSTRAP}"
    )

    print(
        f"Kafka topic : "
        f"{KAFKA_TOPIC}"
    )

    print("=" * 60)


    client = mqtt.Client(
        callback_api_version=(
            mqtt.CallbackAPIVersion.VERSION2
        ),
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv5,
    )


    client.on_connect = on_connect

    client.on_message = on_message

    client.on_disconnect = on_disconnect


    # --------------------------------------------------------
    # Connect to HiveMQ
    # --------------------------------------------------------

    while True:

        try:

            print(
                "[MQTT] Connecting..."
            )

            client.connect(
                MQTT_HOST,
                MQTT_PORT,
                keepalive=60,
            )

            break

        except Exception as e:

            print(
                f"[MQTT] Connection failed: {e}"
            )

            print(
                "[MQTT] Retrying in 5 seconds..."
            )

            time.sleep(5)


    try:

        client.loop_forever()

    except KeyboardInterrupt:

        print(
            "\n[INFO] Shutting down bridge..."
        )

    finally:

        kafka_producer.flush()

        kafka_producer.close()

        client.disconnect()

        print(
            "[INFO] Bridge stopped."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
