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

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ElasticsearchSinkBuilder<T> {
    private String host;

    private int port;

    private String username;

    private String password;

    private Long threshold;

    private Emitter<T> emitter;

    /**
     * setHost
     * set the host where the Elasticsearch cluster is reachable
     *
     * @param host the host address
     * @return this builder
     */
    public ElasticsearchSinkBuilder<T> setHost(String host) {
        checkNotNull(host);
        checkState(host.length() > 0, "Host cannot be empty");
        this.host = host;
        return this;
    }

    /**
     * setPort
     * set the port where the Elasticsearch cluster is reachable
     *
     * @param port the port number
     * @return
     */
    public ElasticsearchSinkBuilder<T> setPort(int port) {
        checkNotNull(port);
        checkState(host.length() > 0, "Port cannot be empty");
        this.port = port;
        return this;
    }

    /**
     * setUsername
     * set the username to authenticate the connection with the Elasticsearch cluster
     *
     * @param username the auth username
     * @return
     */
    public ElasticsearchSinkBuilder<T> setUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    /**
     * setPassword
     * set the password to authenticate the connection with the Elasticsearch cluster
     *
     * @param password the auth password
     * @return
     */
    public ElasticsearchSinkBuilder<T> setPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    /**
     * setThreshold
     * set the threshold of the internal buffer
     *
     * @param threshold number of items to be buffered
     * @return
     */
    public ElasticsearchSinkBuilder<T> setThreshold(Long threshold) {
        checkNotNull(threshold);
        checkState(threshold > 0, "Threshold cannot be negative");
        this.threshold = threshold;
        return this;
    }

    /**
     * setEmitter
     * set the emitter that will be called at every stream element to be processed and buffered
     *
     * @param emitter emitter operation
     * @return
     */
    public ElasticsearchSinkBuilder<T> setEmitter(Emitter<T> emitter) {
        checkNotNull(emitter);
        this.emitter = emitter;
        return this;
    }

    /**
     * build
     * the Elasticsearch sink
     *
     * @return the {ElasticsearchSink} instance
     */
    public ElasticsearchSink<T> build() throws IOException {
        validate();

        return new ElasticsearchSink<T>(
            new NetworkConfigFactory(host, port, username, password),
            emitter,
            threshold,
            new BulkRequestFactory()
        );
    }

    public static <T> ElasticsearchSinkBuilder<T> builder() {
        return new ElasticsearchSinkBuilder<>();
    }

    private static void validate() {
        //
    }
}
