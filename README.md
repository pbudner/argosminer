# ArgosMiner

<img src="assets/logo.svg" height="60px" />

ArgosMiner is a process mining system. It collects events from various sources and traces the underlying discovered process models over time. 

The following features distinguish ArgosMiner from other process mining systems:
- foo
- bar

## Architecture Overview
tbd.
- Using tidwall/gjson accessing multiple fields witihn the JSON string
- Badger as an embedded key-value store

### Performance Improvements
- Kafka: Using asynchronous commits (e.g., every second) resulted in a performance of more than 100.000 messages/second (Kafka Source + Raw Parser + Null Receiver), which is more than sufficient for our purposes. Drawback: After a failure, we might ingest duplicate events.
- JSON Parser: From 15.000 messages/second to 

## Install
There are various ways of installing ArgosMiner.

### Precompiled Binaries
Precompiled binaries for released versions are available in the download section.

### Docker Images
Docker images are available on Docker Hub.

### Building from Source
tbd.

## ToDos
- Embedded Graph-DB: https://github.com/krotik/eliasdb
- Have a look on this DB (maybe for own event db implementation): https://github.com/kelindar/talaria
- Have a look on this https://barkeywolf.consulting/posts/badger-event-store/
