
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

package com.mtfelisb.flink.connectors.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ElasticsearchSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private final static Logger LOG = LogManager.getLogger(ElasticsearchSink.class);

    /**
     * elasticsearch client
     * used to send requests to the Elasticsearch cluster
     *
     */
    private ElasticsearchClient esClient;

    /**
     * network config factory
     * used to create connections with Elasticsearch cluster
     *
     */
    private final INetworkConfigFactory networkConfig;

    private BulkRequest.Builder bulkRequest;

    /**
     * bulk request builder
     * recreated on every flushing
     *
     */
    private final IBulkRequestFactory bulkRequestFactory;

    /**
     * counter used to limit bulk operations
     * based on the provided threshold
     *
     */
    private final AtomicLong thresholdCounter = new AtomicLong(0);

    /**
     * user defined bulk size
     *
     */
    private final int threshold;

    /**
     * user defined operations to be bulked
     * emitted on every single stream item
     *
     */
    private final Emitter<T> emitter;

    /**
     * ElasticsearchSink
     *
     * @param networkConfig a factory to create the network conn with Elasticsearch
     * @param emitter user defined operations to be sent in bulk requests
     * @param threshold used defined limiting the bulk request size
     */
    public ElasticsearchSink(INetworkConfigFactory networkConfig, Emitter<T> emitter, int threshold, IBulkRequestFactory bulkRequestFactory) {
        this.networkConfig = networkConfig;
        this.emitter = emitter;
        this.threshold = threshold;
        this.bulkRequestFactory = bulkRequestFactory;
    }

    /**
     * open
     * creates the connection with the Elasticsearch cluster
     * relying on the provided factory
     *
     * @param parameters The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) {
        this.esClient = this.networkConfig.create();
        this.bulkRequest = bulkRequestFactory.create();
    }

    /**
     * invoke
     * buffers every data element in the stream until the threshold is reached
     *
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception
     */
    @Override
    public void invoke(T value, Context context) throws Exception {
        thresholdCounter.getAndAdd(1);
        bulkRequest.operations(op -> this.emitter.emit(value, op));

        if (thresholdCounter.get() == threshold) {
            flush();
        }
    }

    /**
     * flush
     * flushes the buffered data sending requests to Elasticsearch in bulks;
     *
     * @throws IOException
     */
    private void flush() throws IOException {
        if (thresholdCounter.get() == 0) return;
        BulkResponse result = esClient.bulk(bulkRequest.build());

        // Log errors, if any
        if (result.errors()) {
            LOG.error("Bulk had errors");
            for (BulkResponseItem item: result.items()) {
                if (item.error() != null) {
                    LOG.error(item.error().reason());
                }
            }
        }

        bulkRequest = this.bulkRequestFactory.create();
        thresholdCounter.set(0);
        LOG.debug("Ingestion took {}ms of {} items", result.took(), result.items().size());
    }

    /**
     * snapshotState
     * used as a hook; that means every checkpoint will trigger a bulk request to Elasticsearch
     *
     * @param functionSnapshotContext the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        flush();
    }

    /**
     * initializeState
     * @param functionInitializationContext the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        //
    }

    /**
     * close
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        //
    }
}
