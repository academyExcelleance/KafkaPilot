"""
  A MCP server for connecting to Kafka cluster and executing queries
"""

import argparse
from confluent_kafka.admin import AdminClient


# Kafka connection config
KAFKA_CONFIG = {
    "bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "2WB5NV3VPITGFLTO",
    "sasl.password": "cflt0XvVtNYiUAvHwd7fB0eeO2AshmtJ1LrN4FUmbAaW8DrYKw5FYYbG8aQd298w",
    "session.timeout.ms": 45000,
    "client.id": "ccloud-python-client-1b01e746-0600-4b92-be47-33cce6aa91cb"
}

# Create admin client
admin_client = AdminClient(KAFKA_CONFIG)


def list_kafka_topics() -> list:
    """List all Kafka topics using AdminClient"""
    metadata = admin_client.list_topics(timeout=10)
    return list(metadata.topics.keys())


def describe_kafka_topic(topic_name: str) -> dict:
    """Describe a Kafka topic with partitions, replication, and configs"""
    metadata = admin_client.list_topics(topic=topic_name, timeout=10)

    if topic_name not in metadata.topics:
        return {"error": f"Topic '{topic_name}' not found"}

    topic = metadata.topics[topic_name]
    partitions = len(topic.partitions)
    replication_factor = (
        len(topic.partitions[0].replicas) if partitions > 0 else 0
    )

    return {
        "topic": topic_name,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "configs": "Use ConfigResource(AdminClient.describe_configs) for details"
    }


def main():
    """CLI for testing Kafka admin functions"""
    parser = argparse.ArgumentParser(description="Kafka MCP Test CLI")
    subparsers = parser.add_subparsers(dest="command")

    # list command
    subparsers.add_parser("list", help="List all Kafka topics")

    # describe command
    describe_parser = subparsers.add_parser("describe", help="Describe a Kafka topic")
    describe_parser.add_argument("topic", type=str, help="Name of the topic to describe")

    args = parser.parse_args()

    if args.command == "list":
        topics = list_kafka_topics()
        print("📂 Topics:", topics)

    elif args.command == "describe":
        details = describe_kafka_topic(args.topic)
        print("📑 Topic details:", details)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
