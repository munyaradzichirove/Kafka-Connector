import frappe
import json
from kafka import KafkaProducer
import threading

# producer cache
_producers = {}
_lock = threading.Lock()
def get_producer(bootstrap: str) -> KafkaProducer:
    if bootstrap not in _producers:
        with _lock:
            if bootstrap not in _producers:
                # Adding api_version is the fix for your WSL NoBrokersAvailable error
                _producers[bootstrap] = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    api_version=(0, 11, 5),  # Mandatory for Redpanda + kafka-python
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"), # Added default=str for dates
                    key_serializer=lambda v: v.encode("utf-8") if v else None,
                    acks="all",
                    retries=3,
                    linger_ms=5,
                )
    return _producers[bootstrap]

def send_to_kafka(server_doc_name: str, topic: str, payload: dict):
    """
    server_doc_name → Name of Kafka Server DocType
    topic → Kafka topic
    payload → dictionary to send
    """
    try:
        # fetch host and port from Kafka Server
        server_doc = frappe.get_doc("Kafka Server", server_doc_name)
        host = server_doc.host
        port = server_doc.port

        if not host or not port:
            frappe.log_error(f"Kafka server host/port missing for {server_doc_name}", "Kafka Error")
            return

        bootstrap = f"{host}:{port}"
        producer = get_producer(bootstrap)
        key = payload.get("_name")  # same doc goes to same partition

        future = producer.send(topic, key=key, value=payload)

        # async callbacks
        future.add_callback(
            lambda m: print(f"[Kafka ✔] topic={m.topic}, partition={m.partition}, offset={m.offset}")
        )
        future.add_errback(
            lambda e: print(f"[Kafka ✖ ERROR] {str(e)}")
        )

    except Exception as e:
        print(f"[Kafka FATAL] {str(e)}")
EVENT_MAP = {
    "After Insert": "after_insert",
    "On Update": "on_update",
    "On Submit": "on_submit",
    "On Cancel": "on_cancel",
    "On Trash": "on_trash"
}

def handle_event(doc, method):
    print(f"----------triggered for {doc}")
    
    # Find enabled connectors for this DocType AND matching event
    connectors = frappe.get_all(
        "Kafka Connector",
        filters={"enabled": 1, "doctype_used": doc.doctype}
    )

    for c in connectors:
        connector = frappe.get_doc("Kafka Connector", c.name)

        # Map the user-friendly event to backend
        connector_event = EVENT_MAP.get(connector.event)
        if connector_event != method:
            continue  # skip this connector if it doesn't match the current event

        print(f"[Kafka Connector] Sending for event: {method}, doc: {doc.name}")
        # Build payload...
        if connector.include_full_doc:
            payload = doc.as_dict()
        else:
            payload = {
                f.field_name: doc.get(f.field_name)
                for f in connector.connector_fields
                if f.include
            }
        print(payload)
        # Add metadata
        payload["_event"] = method
        payload["_doctype"] = doc.doctype
        payload["_name"] = doc.name

   
        send_to_kafka(connector.server, connector.topic, payload)