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
import org.apache.http.HttpHost;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import org.junit.jupiter.api.Test;
import java.io.IOException;

@Testcontainers
public class NetworkConfigFactoryTest extends ElasticsearchSinkBaseITCase {
    /**
     * Do not create instances if no host is provided
     *
     */
    @Test
    public void requiredHost() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> new NetworkConfigFactory(null, "username", "password"));

        assertEquals(null, exception.getMessage());
    }

    /**
     * Create a simple instance of ElasticsearchClient
     * without username and password
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        ElasticsearchClient esClient = new NetworkConfigFactory(
            HttpHost.create(ES_CONTAINER.getHttpHostAddress()),
            null,
            null
        ).create();

        assertEquals(true, esClient.ping().value());
    }

    /**
     * Create a simple instance of ElasticsearchClient
     * with username and password
     *
     * @throws IOException
     */
    @Test
    public void createWithUsernameAndPassword() throws IOException {
        ElasticsearchClient esClient = new NetworkConfigFactory(
            HttpHost.create(ES_CONTAINER.getHttpHostAddress()),
            "elastic",
            "generic-pass"
        ).create();

        assertEquals(true, esClient.ping().value());
    }
}
