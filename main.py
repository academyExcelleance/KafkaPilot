"""
  A MCP server for connecting to Kafka cluster and executing queries
"""

import os
from dotenv import load_dotenv

from mcp.server.fastmcp import FastMCP


load_dotenv()

mcp = FastMCP(
    name="kafka"
)


@mcp.tool("list_kafka_topics")
def list_kafka_topics() -> list:
    # Placeholder function to list Kafka topics
    return ["topic1", "topic2", "topic3"]

@mcp.tool("describe_kafka_topic")
def describe_kafka_topic(topic_name: str) -> dict:
    # Placeholder function to describe a Kafka topic
    return {
        "topic": topic_name,
        "partitions": 3,
        "replication_factor": 2,
        "configs": {
            "cleanup.policy": "delete",
            "compression.type": "producer"
        }
    }


def main():
    """Run the MCP server"""
    print("🌤️ Starting Kafka MCP Server...")
    print("📍 Supported functionalities:")
    print("  - List Kafka topics (list_kafka_topics)")
    print("  - Describe a Kafka topic (describe_kafka_topic)")
    
    mcp.run()
    
    print("🚀 Server is running and ready to accept requests.")


if __name__ == "__main__":
    main()
