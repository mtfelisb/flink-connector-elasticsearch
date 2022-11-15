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

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ElasticsearchSinkBuilderTest {

    /**
     * Do not allow invalid Sink objects to be created;
     * The validate method should guarantee that all
     * required properties are in place with valid values
     *
     */
    @Test
    public void sinkBuilderValidate() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder().build());

        assertEquals(null, exception.getMessage());
    }

    /**
     * Emitter should be declared to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetEmitter() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setPort(9200)
                .setHost("localhost")
                .setPassword("password")
                .setUsername("username")
                .setEmitter(null)
                .setThreshold(1L)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The host should be declared to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetHost() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setHost(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The host should be declared to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetHostEmpty() {
        Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setHost("")
                .build()
        );

        assertEquals("Host cannot be empty", exception.getMessage());
    }

    /**
     * The username, if provided, cannot be null to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetUsername() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setUsername(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The password, if provided, cannot be null to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetPassword() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setPassword(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The threshold cannot be null to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetThreshold() {
        Throwable exception = assertThrows(
            NullPointerException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setThreshold(null)
                .build()
        );

        assertEquals(null, exception.getMessage());
    }

    /**
     * The threshold cannot be negative to create
     * a valid ElasticsearchSink instance
     *
     */
    @Test
    public void sinkBuilderSetThresholdNegative() {
        Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> ElasticsearchSinkBuilder.<String>builder()
                .setThreshold(-1L)
                .build()
        );

        assertEquals("Threshold should be positive", exception.getMessage());
    }
}
