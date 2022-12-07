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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import com.mtfelisb.flink.connectors.elasticsearch.sink.v2.ElasticsearchWriter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkNotNull;

/**
 * NetworkConfigFactory
 *
 * A factory that implements the INetworkConfigFactory interface
 * to create valid ElasticsearchClient instances
 */
public class NetworkConfigFactory implements INetworkConfigFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkConfigFactory.class);

    private String host;

    private int port;

    private HttpHost[] hosts;

    private final String username;

    private final String password;

    public NetworkConfigFactory(String host, int port, String username, String password) {
        this.host = checkNotNull(host);
        this.port = checkNotNull(port);
        this.username = username;
        this.password = password;
    }

    public NetworkConfigFactory(HttpHost[] hosts, String username, String password) {
        this.hosts = checkNotNull(hosts);
        this.username = username;
        this.password = password;
    }

    @Override
    public ElasticsearchClient create() {
        LOG.debug("Http client host {} and port {}", host, port);
        return new ElasticsearchClient(
            new RestClientTransport(this.getRestClient(), new JacksonJsonpMapper()));
    }

    @Override
    public ElasticsearchAsyncClient createAsync() {
        LOG.debug("Http client host {} and port {}", host, port);
        return new ElasticsearchAsyncClient(
            new RestClientTransport(this.getRestClient(), new JacksonJsonpMapper()));
    }

    private RestClient getRestClient() {
        if (host != null) {
            return RestClient.builder(new HttpHost(host, port))
                .setHttpClientConfigCallback(httpClientBuilder ->
                    (username != null && password != null) ?
                        httpClientBuilder.setDefaultCredentialsProvider(getCredentials())
                        : httpClientBuilder
                ).build();
        }

        return RestClient.builder(hosts)
            .setHttpClientConfigCallback(httpClientBuilder ->
                (username != null && password != null) ?
                    httpClientBuilder.setDefaultCredentialsProvider(getCredentials())
                    : httpClientBuilder
            ).build();
    }

    private CredentialsProvider getCredentials() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));

        return credentialsProvider;
    }
}
