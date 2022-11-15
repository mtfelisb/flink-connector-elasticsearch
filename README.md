# flink-connector-elasticsearch
Apache Flink connector to Elasticsearch v8

![Publish](https://github.com/mtfelisb/flink-connector-elasticsearch/workflows/Publish/badge.svg)
![CI](https://github.com/mtfelisb/flink-connector-elasticsearch/workflows/CI/badge.svg)

## The what & why
This project aims to fill the gap between Apache Flink and Elasticsearch version 8. Officially, the Flink project has connectors to Elasticsearch versions 6 and 7, and they share the same base. However, the latest version of Elasticsearch count on a brand new Java API Client, instead of the now deprecated `RestHighLevelClient`. That's the main reason to create this connector from scratch.

Similarly to the previous versions, internally each parallel instance uses a buffer to send requests to the Elasticsearch cluster in bulk. The buffer can be flushed by the threshold or enabling the checkpointing.

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
DataStream<T> stream = ...

final ElasticsearchSink<T> esSink = ElasticsearchSinkBuilder.<T>builder()
    .setThreshold(100L)
    .setHost("localhost")
    .setPort(9200)
    .setEmitter(
        (element, operation, context) ->
            (BulkOperation.Builder) operation
              .update(up -> up
                .id(element.getId())
                .index(ELASTICSEARCH_INDEX)
                .action(ac -> ac.doc(element.getValue()))
              )
    )
    .build();
    
  stream.addSink(esSink);
  
  env.execute();
```


## Delivery Guarantee
The `flink-connector-elasticsearch` is integrated with Flink's checkpointing mechanism, meaning that it will flush all buffered data into the Elasticsearch cluster when the checkpoint is triggered automatically. Hence, `flink-connector-elasticsearch` holds `AT_LEAST_ONCE` guarantee when the checkpoint is enabled.

**Important:** The `EXACTLY_ONCE` guarantee can also be achieved if the update operation holds deterministic ids and the upsert is flagged true.

## Sink options

| Name          | Type          | Required      | Description   |
| ------------- | ------------- | ------------- | ------------- |
| threshold     | Long          | Yes           | The internal buffer limit to send requests |
| host          | String        | Yes           | The host to reach the Elasticsearch cluster |
| port          | int           | Yes           | The port to reach the Elasticsearch cluster |
| emitter       | Emitter<>     | Yes           | The Emitter implementation to process each element before pushing to the buffer |
| username      | String        | No            | The username to authenticate in the Elasticsearch cluster |
| password      | String        | No            | The password to authenticate in the Elasticsearch cluster |

## Changelog
- Initial release

## License
See the LICENSE [file](https://github.com/mtfelisb/flink-connector-elasticsearch/blob/main/LICENSE) for more details.
