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

import org.apache.http.HttpHost;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The builder responsible to create valid ElasticsearchSink
 * instances
 *
 * @param <T>
 */
public class ElasticsearchSinkBuilder<T> {
    /**
     * The HttpHost where the Elasticsearch cluster is reachable
     *
     */
    private HttpHost httpHost;

    /**
     * The username to authenticate the connection with the Elasticsearch cluster
     *
     */
    private String username;

    /**
     * The password to authenticate the connection with the Elasticsearch cluster
     *
     */
    private String password;

    /**
     * The threshold of the internal buffer
     *
     */
    private Long threshold;

    /**
     * The emitter that will be called on every stream element to be processed and buffered
     *
     */
    private Emitter<T> emitter;

    /**
     * setHttpHost
     * set the HttpHost where the Elasticsearch cluster is reachable
     *
     * @param httpHost the host address
     * @return this builder
     */
    public ElasticsearchSinkBuilder<T> setHttpHost(HttpHost httpHost) {
        checkNotNull(httpHost);
        this.httpHost = httpHost;
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
        checkState(threshold >= 0, "Threshold should be positive");
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
    public ElasticsearchSink<T> build() {
        validate();

        return new ElasticsearchSink<T>(
            new NetworkConfigFactory(httpHost, username, password),
            emitter,
            threshold,
            new BulkRequestFactory()
        );
    }

    public static <T> ElasticsearchSinkBuilder<T> builder() {
        return new ElasticsearchSinkBuilder<>();
    }

    private void validate() {
        this.setEmitter(this.emitter);
        this.setHttpHost(this.httpHost);
        this.setThreshold(this.threshold);
    }
}
