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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.http.HttpHost;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Random;

@Testcontainers
public class ElasticsearchSinkTest {
    public static final String ELASTICSEARCH_VERSION = "8.5.0";

    public static final DockerImageName ELASTICSEARCH_IMAGE = DockerImageName
            .parse("docker.elastic.co/elasticsearch/elasticsearch")
            .withTag(ELASTICSEARCH_VERSION);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    @Container
    private static final ElasticsearchContainer ES_CONTAINER = createElasticsearchContainer();

    public static ElasticsearchContainer createElasticsearchContainer() {
        return new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
            .withEnv("xpack.security.enabled", "false");
    }

    public RestClient client;

    public ElasticsearchClient esClient;

    @BeforeEach
    void setUp() {
        this.client = RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())).build();
        this.esClient = new ElasticsearchClient(new RestClientTransport(RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())).build(), new JacksonJsonpMapper()));
    }

    @Test
    public void indexingByThresholdReached() throws Exception {
        String INDEX_NAME = "threshold-reached";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final ElasticsearchSink<DummyData> sink = ElasticsearchSinkBuilder.<DummyData>builder()
            .setThreshold(2L)
            .setEmitter(
                (value, op) ->
                    (BulkOperation.Builder) op.update(up -> up.id(value.getId()).index(INDEX_NAME).action(ac -> ac.doc(value).docAsUpsert(true)))
            )
            .setHost(ES_CONTAINER.getHost())
            .setPort(ES_CONTAINER.getFirstMappedPort())
            .build();

        env
            .fromElements("first", "second", "third")
            .map((MapFunction<String, DummyData>) value -> new DummyData(new Random().nextLong() + "_v1", value))
            .addSink(sink);

        env.execute();

        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", INDEX_NAME + "/_search/"));
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
    }

    @Test
    public void indexingByCheckpoint() throws Exception {
        String INDEX_NAME = "checkpoint-triggered";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);

        final ElasticsearchSink<DummyData> sink = ElasticsearchSinkBuilder.<DummyData>builder()
            .setThreshold(1000L)
            .setEmitter(
                (value, op) ->
                    (BulkOperation.Builder) op.index(i -> i.id(value.getId()).document(value).index(INDEX_NAME))
            )
            .setHost(ES_CONTAINER.getHost())
            .setPort(ES_CONTAINER.getFirstMappedPort())
            .build();

        env
            .fromElements("first", "second")
            .map((MapFunction<String, DummyData>) value -> new DummyData(new Random().nextLong() + "_v1", value))
            .addSink(sink);

        env.execute();

        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", INDEX_NAME + "/_search/"));
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
    }

    public static class DummyData {
        private String id;

        private String name;

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
