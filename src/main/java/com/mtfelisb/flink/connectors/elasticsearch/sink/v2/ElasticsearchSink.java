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

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public class ElasticsearchSink<InputT> extends AsyncSinkBase<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

    private final String username;

    private final String password;

    private final HttpHost[] httpHosts;

    protected ElasticsearchSink(
        ElementConverter<InputT, Operation> converter,
        int maxBatchSize,
        int maxInFlightRequests,
        int maxBufferedRequests,
        long maxBatchSizeInBytes,
        long maxTimeInBufferMS,
        long maxRecordSizeInByte,
        String username,
        String password,
        HttpHost[] httpHosts
    ) {
        super(
            converter,
            maxBatchSize,
            maxInFlightRequests,
            maxBufferedRequests,
            maxBatchSizeInBytes,
            maxTimeInBufferMS,
            maxRecordSizeInByte
        );

        this.username = username;
        this.password = password;
        this.httpHosts = httpHosts;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> createWriter(
        InitContext context
    ) {
        LOG.debug("creating writer");

        return new ElasticsearchWriter<>(
            getElementConverter(),
            context,
            getMaxBatchSize(),
            getMaxInFlightRequests(),
            getMaxBufferedRequests(),
            getMaxBatchSizeInBytes(),
            getMaxTimeInBufferMS(),
            getMaxRecordSizeInBytes(),
            username,
            password,
            httpHosts,
            Collections.emptyList()
        );
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> restoreWriter(
        InitContext context,
        Collection<BufferedRequestState<Operation>> recoveredState
    ) {
        LOG.debug("restoring writer with {} items", recoveredState.size());

        return new ElasticsearchWriter<>(
            getElementConverter(),
            context,
            getMaxBatchSize(),
            getMaxInFlightRequests(),
            getMaxBufferedRequests(),
            getMaxBatchSizeInBytes(),
            getMaxTimeInBufferMS(),
            getMaxRecordSizeInBytes(),
            username,
            password,
            httpHosts,
            recoveredState
        );
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Operation>> getWriterStateSerializer() {
        return new ElasticsearchSinkSerializer();
    }
}
