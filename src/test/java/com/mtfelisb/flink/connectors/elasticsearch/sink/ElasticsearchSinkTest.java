/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.mtfelisb.flink.connectors.elasticsearch.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ElasticsearchSinkTest extends ElasticsearchSinkBaseITCase {
    @BeforeEach
    void setUp() {
        this.client = RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())).build();
        this.esClient = new ElasticsearchClient(new RestClientTransport(RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())).build(), new JacksonJsonpMapper()));
    }

    /**
     * indexingByThresholdReached
     * It's expected to sink data when the threshold specified is reached
     *
     * @throws Exception
     */
    @Test
    public void indexingByThresholdReached() throws Exception {
        String ELASTICSEARCH_INDEX_NAME = "threshold-reached";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment()
            .setParallelism(1);

        final ElasticsearchSink<DummyData> sink = ElasticsearchSinkBuilder.<DummyData>builder()
            .setThreshold(2L)
            .setHost(ES_CONTAINER.getHost())
            .setPort(ES_CONTAINER.getFirstMappedPort())
            .setEmitter(
                (value, op, ctx) ->
                    (BulkOperation.Builder) op
                        .update(up -> up
                            .id(value.getId())
                            .index(ELASTICSEARCH_INDEX_NAME)
                            .action(ac -> ac
                                .doc(value)
                                .docAsUpsert(true)
                            )
                        )
            )
            .build();

        env
            .fromElements("first", "second", "third")
            .map((MapFunction<String, DummyData>) value -> new DummyData(value + "_v1_index", value))
            .addSink(sink);

        env.execute();

        assertIdsAreWritten(ELASTICSEARCH_INDEX_NAME, new String[]{"first_v1_index", "second_v1_index"});
    }

    /**
     * indexingByCheckpoint
     * It's expected to sink data when the checkpoint is triggered
     *
     * @throws Exception
     */
    @Test
    public void indexingByCheckpoint() throws Exception {
        String ELASTICSEARCH_INDEX_NAME = "checkpoint-triggered";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment()
            .enableCheckpointing(500);

        final ElasticsearchSink<DummyData> sink = ElasticsearchSinkBuilder.<DummyData>builder()
            .setThreshold(1000L)
            .setHost(ES_CONTAINER.getHost())
            .setPort(ES_CONTAINER.getFirstMappedPort())
            .setEmitter(
                (value, op, ctx) ->
                    (BulkOperation.Builder) op
                        .index(i -> i
                            .id(value.getId())
                            .document(value)
                            .index(ELASTICSEARCH_INDEX_NAME)
                        )
            )
            .build();

        env
            .fromElements("first", "second")
            .map((MapFunction<String, DummyData>) value -> new DummyData(value + "_v1_index", value))
            .addSink(sink);

        env.execute();

        assertIdsAreWritten(ELASTICSEARCH_INDEX_NAME, new String[]{"first_v1_index", "second_v1_index"});
    }

    public static class DummyData {
        private final String id;

        private final String name;

        public DummyData(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getId() {
            return id;
        }
    }
}
