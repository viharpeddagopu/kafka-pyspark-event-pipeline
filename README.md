# Scalable Event Processing Pipeline (Kafka + PySpark)

## Overview
This project demonstrates a prototype implementation of real-time event processing pipeline using Apache Kafka and PySpark. It simulates booking events and processes them using a streaming architecture.

## Architecture
Producer → Kafka Topic → PySpark Streaming → Aggregation

## Features
- Simulates real-time booking events
- Uses Kafka as a message broker for decoupling producers and consumers
- Processes streaming data using PySpark Structured Streaming
- Computes aggregated metrics such as total tickets and revenue per route

## Tech Stack
- Apache Kafka
- PySpark
- Python

## Use Case
Inspired by real-world ticket booking systems, this project demonstrates how event-driven architectures enable scalable and real-time data processing.
