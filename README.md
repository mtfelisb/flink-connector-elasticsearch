# flink-connector-elasticsearch
Apache Flink connector to Elasticsearch v8

![Publish](https://github.com/mtfelisb/flink-connector-elasticsearch/workflows/Publish/badge.svg)
![CI](https://github.com/mtfelisb/flink-connector-elasticsearch/workflows/CI/badge.svg)

## The project
This project aims to fill the gap between Apache Flink and Elasticsearch version 8.

## Getting started

### Add this to pom.xml:

```xml
<dependency>
  <groupId>com.mtfelisb</groupId>
  <artifactId>flink-connector-elasticsearch</artifactId>
  <version>1.0.0</version>
</dependency>

```

### Run via command line:

```
mvn install
```

### Gradle
```
implementation com.mtfelisb:flink-connector-elasticsearch:1.0.0
```

## Example

```java
DataStream<DummyData> stream = ...

final ElasticsearchSink<DummyData> esSink = ElasticsearchSinkBuilder.<DummyData>builder()
    .setThreshold(2L)
    .setEmitter(
        (element, operation, context) ->
            (BulkOperation.Builder) operation
              .update(up -> up
                .id(element.getId())
                .index(ELASTICSEARCH_INDEX)
                .action(ac -> ac.doc(element.getValue()))
              )
    )
    .setHost(ES_CONTAINER.getHost())
    .setPort(ES_CONTAINER.getFirstMappedPort())
    .build();
    
  stream.addSink(esSink);
  
  env.execute();
```

## Delivery Guarantee
The `flink-connector-elasticsearch` is integrated with Flink's checkpointing mechanism, meaning that it will flush all buffered data into the Elasticsearch cluster when the checkpoint is triggered automatically. Hence, `flink-connector-elasticsearch` holds `AT_LEAST_ONCE` guarantee when the checkpoint is enabled.

**Important:** The `EXACTLY_ONCE` guarantee can also be achieved if the update operation holds deterministic ids and the upsert is flagged true.

## Changelog
- Initial release

## License
See the LICENSE [file](https://github.com/mtfelisb/flink-connector-elasticsearch/blob/main/LICENSE) for more details.
