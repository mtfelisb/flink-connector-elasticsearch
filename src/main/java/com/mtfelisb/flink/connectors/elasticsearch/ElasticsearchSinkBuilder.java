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

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ElasticsearchSinkBuilder<T> {
    private String host;

    private int port;

    private String username;

    private String password;

    public ElasticsearchSinkBuilder<T> withHost(String host) {
        checkNotNull(host);
        checkState(host.length() > 0, "Host cannot be empty");
        this.host = host;
        return this;
    }

    public ElasticsearchSinkBuilder<T> withPort(int port) {
        checkNotNull(port);
        checkState(host.length() > 0, "Port cannot be empty");
        this.port = port;
        return this;
    }

    public ElasticsearchSinkBuilder<T> withUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    public ElasticsearchSinkBuilder<T> withPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    public ElasticsearchSink<T> build() {
        validate();

        return new ElasticsearchSink<T>(
            new NetworkConfigFactory(host, port, username, password)
        );
    }

    public static <T> ElasticsearchSinkBuilder<T> builder() {
        return new ElasticsearchSinkBuilder<>();
    }

    private static void validate() {
        //
    }
}
