#!/usr/bin/env python3
"""KafkaPilot with LangChain Agentic AI (REPL-based)

This version runs as a REPL instead of FastAPI. Users can type natural
language queries, the agent decides which Kafka/Prometheus tool to use,
fetches real data, and then summarizes/analyzes results with the LLM.

Key features:
- KafkaAdminClient + KafkaConsumer for metadata
- Prometheus API queries for lag/throughput
- SecurityValidator to block dangerous ops
- LangChain ReAct agent with StructuredTools
- REPL interface that prints raw tool outputs and LLM summarized answers
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List

import requests
import yaml
from pydantic import BaseSettings, Field

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

from langchain.agents import initialize_agent, AgentType
from langchain.tools import StructuredTool
from langchain_openai import ChatOpenAI

# --------- Configuration ---------------------------------

class KafkaPilotConfig(BaseSettings):
    kafka_bootstrap_servers: str = Field(default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    prometheus_url: str = Field(default=os.getenv("PROMETHEUS_URL", "http://localhost:9090"))
    security_protocol: str = Field(default=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    sasl_mechanism: Optional[str] = Field(default=os.getenv("KAFKA_SASL_MECHANISM"))
    sasl_username: Optional[str] = Field(default=os.getenv("KAFKA_SASL_USERNAME"))
    sasl_password: Optional[str] = Field(default=os.getenv("KAFKA_SASL_PASSWORD"))

    allowed_operations: List[str] = Field(default_factory=lambda: [
        "list_topics", "describe_topic", "list_consumer_groups",
        "get_consumer_lag", "get_throughput_metrics"
    ])
    dangerous_operations: List[str] = Field(default_factory=lambda: [
        "delete_topic", "reset_consumer_offsets", "create_topic"
    ])

    class Config:
        env_file = ".env"

    @classmethod
    def from_yaml(cls, path: Optional[str]):
        if not path:
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

# --------- Security / Validation --------------------------

class SecurityValidator:
    def __init__(self, cfg: KafkaPilotConfig):
        self.cfg = cfg
        self.dangerous = set(cfg.dangerous_operations)

    def validate_operation(self, operation: str) -> bool:
        return operation in self.cfg.allowed_operations and operation not in self.dangerous

    @staticmethod
    def sanitize_topic_name(topic: str) -> str:
        allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
        return "".join(c for c in topic if c in allowed_chars)

# --------- Kafka Agent -----------------------------------

class KafkaAgent:
    def __init__(self, cfg: KafkaPilotConfig):
        self.cfg = cfg
        self.admin_client: Optional[KafkaAdminClient] = None
        self.consumer: Optional[KafkaConsumer] = None
        self._setup_clients()

    def _client_config(self) -> Dict[str, Any]:
        cfg = {"bootstrap_servers": self.cfg.kafka_bootstrap_servers, "security_protocol": self.cfg.security_protocol}
        if self.cfg.sasl_mechanism:
            cfg.update({
                "sasl_mechanism": self.cfg.sasl_mechanism,
                "sasl_plain_username": self.cfg.sasl_username,
                "sasl_plain_password": self.cfg.sasl_password,
            })
        return cfg

    def _setup_clients(self):
        try:
            cfg = self._client_config()
            self.admin_client = KafkaAdminClient(**cfg)
            self.consumer = KafkaConsumer(**cfg, consumer_timeout_ms=2000)
        except Exception as e:
            logging.error("Kafka client setup failed: %s", e)

    def list_topics(self) -> Dict[str, Any]:
        try:
            topics = self.admin_client.list_topics()
            return {"topics": list(topics)}
        except Exception as e:
            return {"error": str(e)}

    def describe_topic(self, topic_name: str) -> Dict[str, Any]:
        topic_name = SecurityValidator.sanitize_topic_name(topic_name)
        try:
            descs = self.admin_client.describe_topics([topic_name])
            return {"description": str(descs)}
        except Exception as e:
            return {"error": str(e)}

    def list_consumer_groups(self) -> Dict[str, Any]:
        try:
            groups = self.admin_client.list_consumer_groups()
            return {"consumer_groups": [str(g) for g in groups]}
        except Exception as e:
            return {"error": str(e)}

# --------- Metrics Agent ---------------------------------

class MetricsAgent:
    def __init__(self, cfg: KafkaPilotConfig):
        self.prometheus_url = cfg.prometheus_url

    def query_prometheus(self, query: str) -> Dict[str, Any]:
        try:
            url = f"{self.prometheus_url}/api/v1/query"
            resp = requests.get(url, params={"query": query}, timeout=10)
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    def get_consumer_lag(self, group_id: Optional[str] = None, topic_name: Optional[str] = None) -> Dict[str, Any]:
        filters = []
        if group_id:
            filters.append(f'group="{group_id}"')
        if topic_name:
            filters.append(f'topic="{topic_name}"')
        fstr = "{" + ",".join(filters) + "}" if filters else ""
        query = f"sum(kafka_consumergroup_lag{fstr})"
        return self.query_prometheus(query)

    def get_throughput_metrics(self, topic_name: Optional[str] = None) -> Dict[str, Any]:
        fstr = f'{{topic="{topic_name}"}}' if topic_name else ""
        msg_rate = f"rate(kafka_server_brokertopicmetrics_messagesin_total{fstr}[5m])"
        byte_rate = f"rate(kafka_server_brokertopicmetrics_bytesin_total{fstr}[5m])"
        return {
            "message_rate": self.query_prometheus(msg_rate),
            "byte_rate": self.query_prometheus(byte_rate),
        }

# --------- LangChain Tools -------------------------------

def build_tools(cfg: KafkaPilotConfig):
    kafka = KafkaAgent(cfg)
    metrics = MetricsAgent(cfg)
    validator = SecurityValidator(cfg)

    tools = []

    if validator.validate_operation("list_topics"):
        tools.append(StructuredTool.from_function(
            kafka.list_topics,
            name="list_topics",
            description="List all Kafka topics"
        ))

    if validator.validate_operation("describe_topic"):
        tools.append(StructuredTool.from_function(
            kafka.describe_topic,
            name="describe_topic",
            description="Describe a Kafka topic",
        ))

    if validator.validate_operation("list_consumer_groups"):
        tools.append(StructuredTool.from_function(
            kafka.list_consumer_groups,
            name="list_consumer_groups",
            description="List all consumer groups",
        ))

    if validator.validate_operation("get_consumer_lag"):
        tools.append(StructuredTool.from_function(
            metrics.get_consumer_lag,
            name="get_consumer_lag",
            description="Get consumer lag from Prometheus",
        ))

    if validator.validate_operation("get_throughput_metrics"):
        tools.append(StructuredTool.from_function(
            metrics.get_throughput_metrics,
            name="get_throughput_metrics",
            description="Get throughput metrics from Prometheus",
        ))

    return tools

# --------- REPL Entrypoint -------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cfg = KafkaPilotConfig()
    tools = build_tools(cfg)
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    agent = initialize_agent(
        tools,
        llm,
        agent=AgentType.OPENAI_FUNCTIONS,
        verbose=True,
        return_intermediate_steps=True,
    )

    print("KafkaPilot REPL ready. Type 'exit' to quit.")
    while True:
        try:
            q = input("User: ").strip()
            if q.lower() in {"exit", "quit"}:
                break
            result = agent(q)
            raw = [step[1] for step in result["intermediate_steps"]]
            print("\n[RAW OUTPUT]", json.dumps(raw, indent=2))
            print("[ANSWER]", result["output"], "\n")
        except KeyboardInterrupt:
            break
