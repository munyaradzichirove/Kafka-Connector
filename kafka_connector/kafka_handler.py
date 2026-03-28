import frappe
import json

def send_to_kafka(server, topic, payload):
    """
    Bro-level placeholder: actual Kafka/Redpanda producer code goes here.
    """
    print(f"[Kafka] Server: {server}, Topic: {topic}, Payload: {json.dumps(payload)}")


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

        # # send to kafka (placeholder)
        # send_to_kafka(connector.server, connector.topic, payload)