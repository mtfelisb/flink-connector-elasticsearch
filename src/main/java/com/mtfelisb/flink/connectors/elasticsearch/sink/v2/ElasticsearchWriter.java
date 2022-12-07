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

package com.mtfelisb.flink.connectors.elasticsearch.sink.v2;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mtfelisb.flink.connectors.elasticsearch.sink.NetworkConfigFactory;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ElasticsearchWriter<InputT> extends AsyncSinkWriter<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);

    private final ElasticsearchAsyncClient esClient;

    public ElasticsearchWriter(
        ElementConverter<InputT, Operation> elementConverter,
        Sink.InitContext context,
        int maxBatchSize,
        int maxInFlightRequests,
        int maxBufferedRequests,
        long maxBatchSizeInBytes,
        long maxTimeInBufferMS,
        long maxRecordSizeInBytes,
        String username,
        String password,
        HttpHost[] httpHosts
    ) {
        super(
            elementConverter,
            context,
            maxBatchSize,
            maxInFlightRequests,
            maxBufferedRequests,
            maxBatchSizeInBytes,
            maxTimeInBufferMS,
            maxRecordSizeInBytes
        );
        LOG.info("creating writer");
        this.esClient = new NetworkConfigFactory(httpHosts, username, password).createAsync();
    }

    @Override
    protected void submitRequestEntries(List<Operation> requestEntries, Consumer<List<Operation>> requestResult) {
        LOG.info("submitRequestEntries {} items", requestEntries.size());

        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        ArrayList<BulkOperation> bulkOperations = requestEntries.stream()
            .map(Operation::getBulkOperation)
            .collect(Collectors.toCollection(ArrayList::new));

        /**
         * Unfortunately, Elasticsearch Client doesn't support
         * retries yet nor does a way to see a failed operation;
         *
         * @see https://github.com/elastic/elasticsearch-java/issues/356
         */
        esClient.bulk(bulkRequest.operations(bulkOperations).build())
            .whenComplete((response, exception) -> {
                // All batch have failed
                if (exception != null) {
                    requestResult.accept(requestEntries);
                    LOG.error("The batch has failed", exception);

                // A few batch items have failed
                } else if (response.errors()) {
                    response.items().stream()
                        .filter(item -> item.error() != null)
                        .forEach(item -> LOG.error("Failed item: {}", item));

                    requestResult.accept(Collections.emptyList());

                // The whole batch were written successfully
                } else {
                    LOG.info("Bulk succeed {} items", requestEntries.size());
                    requestResult.accept(Collections.emptyList());
                }
            });
    }

    @Override
    protected long getSizeInBytes(Operation requestEntry) {
        return requestEntry.toString().getBytes().length;
    }
}

