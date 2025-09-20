#!/usr/bin/env python3

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

# MCP Protocol Implementation
from mcp.server import Server
from mcp.server.models import Tool, TextContent
from mcp.types import CallToolRequest, CallToolResult

# Kafka Client Libraries
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

# Prometheus Client
import requests
from prometheus_client.parser import text_string_to_metric_families

# Security and Validation
import subprocess
import shlex
from pathlib import Path

# Configuration
import yaml
from pydantic import BaseSettings, Field

class KafkaPilotConfig(BaseSettings):
    """Configuration management for KafkaPilot MCP Server"""
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    prometheus_url: str = Field(default="http://localhost:9090")
    security_protocol: str = Field(default="PLAINTEXT")
    sasl_mechanism: Optional[str] = Field(default=None)
    sasl_username: Optional[str] = Field(default=None)
    sasl_password: Optional[str] = Field(default=None)
    ssl_certfile: Optional[str] = Field(default=None)
    ssl_keyfile: Optional[str] = Field(default=None)
    ssl_cafile: Optional[str] = Field(default=None)
    
    # MCP Server Settings
    server_name: str = Field(default="kafka-pilot")
    server_version: str = Field(default="1.0.0")
    
    # Security Settings
    allowed_operations: List[str] = Field(default=[
        "list_topics", "describe_topic", "list_consumer_groups",
        "describe_consumer_group", "get_consumer_lag", "get_throughput_metrics"
    ])
    dangerous_operations: List[str] = Field(default=[
        "delete_topic", "reset_consumer_offsets", "create_topic"
    ])
    
    class Config:
        env_file = ".env"

@dataclass
class KafkaClusterInfo:
    """Kafka cluster connection information"""
    name: str
    bootstrap_servers: str
    security_config: Dict[str, Any]

class SecurityValidator:
    """Validates and sanitizes operations for security"""
    
    def __init__(self, config: KafkaPilotConfig):
        self.config = config
        self.dangerous_ops = set(config.dangerous_operations)
        
    def validate_operation(self, operation: str, params: Dict[str, Any]) -> bool:
        """Validate if operation is allowed"""
        if operation in self.dangerous_ops:
            logging.warning(f"Dangerous operation requested: {operation}")
            return False
        return operation in self.config.allowed_operations
    
    def sanitize_topic_name(self, topic_name: str) -> str:
        """Sanitize topic name to prevent injection"""
        # Remove potentially dangerous characters
        allowed_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.')
        return ''.join(c for c in topic_name if c in allowed_chars)

class KafkaAgent:
    """Agent for Kafka CLI and Admin operations"""
    
    def __init__(self, config: KafkaPilotConfig):
        self.config = config
        self.admin_client = None
        self._setup_admin_client()
    
    def _setup_admin_client(self):
        """Initialize Kafka Admin Client"""
        try:
            client_config = {
                'bootstrap_servers': self.config.kafka_bootstrap_servers,
                'security_protocol': self.config.security_protocol,
            }
            
            if self.config.sasl_mechanism:
                client_config.update({
                    'sasl_mechanism': self.config.sasl_mechanism,
                    'sasl_plain_username': self.config.sasl_username,
                    'sasl_plain_password': self.config.sasl_password,
                })
            
            self.admin_client = KafkaAdminClient(**client_config)
        except Exception as e:
            logging.error(f"Failed to create Kafka Admin Client: {e}")
    
    async def list_topics(self, pattern: Optional[str] = None, 
                         include_internal: bool = False) -> Dict[str, Any]:
        """List Kafka topics"""
        try:
            if not self.admin_client:
                return {"error": "Kafka Admin Client not available"}
            
            metadata = self.admin_client.list_topics()
            topics = []
            
            for topic_name in metadata.topics:
                # Skip internal topics unless requested
                if not include_internal and topic_name.startswith('__'):
                    continue
                
                # Apply pattern filter if provided
                if pattern and pattern not in topic_name:
                    continue
                
                topic_metadata = metadata.topics[topic_name]
                topics.append({
                    'name': topic_name,
                    'partition_count': len(topic_metadata.partitions),
                    'is_internal': topic_name.startswith('__')
                })
            
            return {
                'topics': topics,
                'count': len(topics),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {"error": f"Failed to list topics: {str(e)}"}
    
    async def describe_topic(self, topic_name: str) -> Dict[str, Any]:
        """Describe a specific Kafka topic"""
        try:
            if not self.admin_client:
                return {"error": "Kafka Admin Client not available"}
            
            # Get topic metadata
            metadata = self.admin_client.list_topics()
            if topic_name not in metadata.topics:
                return {"error": f"Topic '{topic_name}' not found"}
            
            topic_metadata = metadata.topics[topic_name]
            
            # Get topic configuration
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            config_result = self.admin_client.describe_configs([config_resource])
            topic_config = config_result[config_resource].resources[config_resource]
            
            return {
                'name': topic_name,
                'partition_count': len(topic_metadata.partitions),
                'partitions': [
                    {
                        'partition_id': p.partition,
                        'leader': p.leader,
                        'replicas': p.replicas,
                        'in_sync_replicas': p.isr
                    }
                    for p in topic_metadata.partitions.values()
                ],
                'configuration': {
                    entry.name: entry.value 
                    for entry in topic_config.configs.values()
                    if not entry.is_default
                }
            }
        except Exception as e:
            return {"error": f"Failed to describe topic: {str(e)}"}
    
    async def list_consumer_groups(self, state: Optional[str] = None) -> Dict[str, Any]:
        """List consumer groups"""
        try:
            if not self.admin_client:
                return {"error": "Kafka Admin Client not available"}
            
            groups = self.admin_client.list_consumer_groups()
            group_list = []
            
            for group in groups:
                if state and group.state != state:
                    continue
                
                group_list.append({
                    'group_id': group.group_id,
                    'state': group.state,
                    'type': group.type
                })
            
            return {
                'consumer_groups': group_list,
                'count': len(group_list)
            }
        except Exception as e:
            return {"error": f"Failed to list consumer groups: {str(e)}"}

class MetricsAgent:
    """Agent for Prometheus metrics queries"""
    
    def __init__(self, config: KafkaPilotConfig):
        self.config = config
        self.prometheus_url = config.prometheus_url
    
    async def query_prometheus(self, query: str, time_range: str = "5m") -> Dict[str, Any]:
        """Execute PromQL query"""
        try:
            url = f"{self.prometheus_url}/api/v1/query"
            params = {"query": query}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data['status'] != 'success':
                return {"error": f"Prometheus query failed: {data.get('error', 'Unknown error')}"}
            
            return {
                'status': 'success',
                'data': data['data'],
                'query': query,
                'timestamp': datetime.now().isoformat()
            }
        except requests.RequestException as e:
            return {"error": f"Failed to query Prometheus: {str(e)}"}
    
    async def get_consumer_lag(self, group_id: Optional[str] = None, 
                             topic_name: Optional[str] = None) -> Dict[str, Any]:
        """Get consumer lag metrics"""
        # Build PromQL query
        filters = []
        if group_id:
            filters.append(f'group="{group_id}"')
        if topic_name:
            filters.append(f'topic="{topic_name}"')
        
        filter_str = "{" + ",".join(filters) + "}" if filters else ""
        query = f"kafka_consumer_lag_sum{filter_str}"
        
        return await self.query_prometheus(query)
    
    async def get_throughput_metrics(self, topic_name: Optional[str] = None) -> Dict[str, Any]:
        """Get throughput metrics for topics"""
        filters = []
        if topic_name:
            filters.append(f'topic="{topic_name}"')
        
        filter_str = "{" + ",".join(filters) + "}" if filters else ""
        
        # Message rate
        message_rate_query = f"rate(kafka_topic_partition_current_offset{filter_str}[5m])"
        message_rate_result = await self.query_prometheus(message_rate_query)
        
        # Byte rate (if available)
        byte_rate_query = f"rate(kafka_topic_partition_log_size{filter_str}[5m])"
        byte_rate_result = await self.query_prometheus(byte_rate_query)
        
        return {
            'message_rate': message_rate_result,
            'byte_rate': byte_rate_result,
            'topic': topic_name
        }

class KafkaPilotMCPServer:
    """Main MCP Server for KafkaPilot"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = KafkaPilotConfig()
        self.security = SecurityValidator(self.config)
        self.kafka_agent = KafkaAgent(self.config)
        self.metrics_agent = MetricsAgent(self.config)
        
        # Initialize MCP Server
        self.server = Server(self.config.server_name)
        self._register_tools()
    
    def _register_tools(self):
        """Register all available MCP tools"""
        
        # Topic Management Tools
        @self.server.call_tool()
        async def list_topics(arguments: Dict[str, Any]) -> List[TextContent]:
            """List Kafka topics"""
            if not self.security.validate_operation("list_topics", arguments):
                return [TextContent(type="text", text="Operation not allowed")]
            
            result = await self.kafka_agent.list_topics(
                pattern=arguments.get("pattern"),
                include_internal=arguments.get("include_internal", False)
            )
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        @self.server.call_tool()
        async def describe_topic(arguments: Dict[str, Any]) -> List[TextContent]:
            """Describe a Kafka topic"""
            if not self.security.validate_operation("describe_topic", arguments):
                return [TextContent(type="text", text="Operation not allowed")]
            
            topic_name = arguments.get("topic_name")
            if not topic_name:
                return [TextContent(type="text", text="Error: topic_name is required")]
            
            # Sanitize topic name
            topic_name = self.security.sanitize_topic_name(topic_name)
            result = await self.kafka_agent.describe_topic(topic_name)
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # Consumer Group Management Tools
        @self.server.call_tool()
        async def list_consumer_groups(arguments: Dict[str, Any]) -> List[TextContent]:
            """List consumer groups"""
            if not self.security.validate_operation("list_consumer_groups", arguments):
                return [TextContent(type="text", text="Operation not allowed")]
            
            result = await self.kafka_agent.list_consumer_groups(
                state=arguments.get("state")
            )
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # Metrics Tools
        @self.server.call_tool()
        async def get_consumer_lag(arguments: Dict[str, Any]) -> List[TextContent]:
            """Get consumer lag metrics"""
            if not self.security.validate_operation("get_consumer_lag", arguments):
                return [TextContent(type="text", text="Operation not allowed")]
            
            result = await self.metrics_agent.get_consumer_lag(
                group_id=arguments.get("group_id"),
                topic_name=arguments.get("topic_name")
            )
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        @self.server.call_tool()
        async def get_throughput_metrics(arguments: Dict[str, Any]) -> List[TextContent]:
            """Get throughput metrics"""
            if not self.security.validate_operation("get_throughput_metrics", arguments):
                return [TextContent(type="text", text="Operation not allowed")]
            
            result = await self.metrics_agent.get_throughput_metrics(
                topic_name=arguments.get("topic_name")
            )
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
    
    async def run(self, host: str = "localhost", port: int = 8000):
        """Run the MCP Server"""
        logging.info(f"Starting KafkaPilot MCP Server on {host}:{port}")
        
        # Server startup logic would go here
        # This depends on the specific MCP server implementation
        pass

# Tool definitions for MCP client discovery
KAFKA_PILOT_TOOLS = [
    Tool(
        name="list_topics",
        description="List all Kafka topics with optional filtering",
        inputSchema={
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Topic name pattern"},
                "include_internal": {"type": "boolean", "description": "Include internal topics"}
            }
        }
    ),
    Tool(
        name="describe_topic",
        description="Get detailed information about a specific topic",
        inputSchema={
            "type": "object",
            "properties": {
                "topic_name": {"type": "string", "description": "Name of the topic"},
            },
            "required": ["topic_name"]
        }
    ),
    Tool(
        name="list_consumer_groups",
        description="List all consumer groups",
        inputSchema={
            "type": "object",
            "properties": {
                "state": {"type": "string", "description": "Filter by group state"}
            }
        }
    ),
    Tool(
        name="get_consumer_lag",
        description="Get consumer lag metrics for topics/groups",
        inputSchema={
            "type": "object",
            "properties": {
                "group_id": {"type": "string", "description": "Consumer group ID"},
                "topic_name": {"type": "string", "description": "Topic name"}
            }
        }
    ),
    Tool(
        name="get_throughput_metrics",
        description="Get topic throughput metrics",
        inputSchema={
            "type": "object",
            "properties": {
                "topic_name": {"type": "string", "description": "Topic name"}
            }
        }
    )
]

# Main entry point
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Load configuration
    server = KafkaPilotMCPServer()
    
    # Run server
    asyncio.run(server.run())