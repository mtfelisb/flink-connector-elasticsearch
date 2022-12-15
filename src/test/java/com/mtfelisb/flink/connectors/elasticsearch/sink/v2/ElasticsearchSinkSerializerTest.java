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

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ElasticsearchSinkSerializerTest {

    @Test
    public void testSerializeAndDeSerialize() throws IOException {
        ElasticsearchSinkSerializer stateSerializer = new ElasticsearchSinkSerializer();

        BufferedRequestState<Operation> state = getTestState(
            (element, context) -> new Operation(element), (element) -> new OperationSerializer().size(element));

        BufferedRequestState<Operation> deserializedState =
            stateSerializer.deserialize(0, stateSerializer.serialize(state));

        assertThatBufferStatesAreEqual(state, deserializedState);
    }

    public static <T extends Serializable> BufferedRequestState<T> getTestState(
        ElementConverter<BulkOperationVariant, T> elementConverter,
        Function<T, Integer> requestSizeExtractor) {
        return new BufferedRequestState<>(
            IntStream.range(0, 100)
                .mapToObj(i -> new IndexOperation.Builder<Dummy>().document(new Dummy(i)).index("test").id("test").build())
                .map(element -> elementConverter.apply(element, null))
                .map(
                    request ->
                        new RequestEntryWrapper<>(
                            request, requestSizeExtractor.apply(request)))
                .collect(Collectors.toList()));
    }

    private static class Dummy {
        private int id;

        Dummy(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "Dummy{" +
                    "id=" + id +
                    '}';
        }
    }

    /**
     * @url <a href="https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/test/java/org/apache/flink/connector/base/sink/writer/AsyncSinkWriterTestUtils.java#L46">...</a>
     * @param actual
     * @param expected
     * @param <T>
     */
    public static <T extends Serializable> void assertThatBufferStatesAreEqual(
            BufferedRequestState<T> actual, BufferedRequestState<T> expected) {
        // Equal states must have equal sizes
        assertThat(actual.getStateSize()).isEqualTo(expected.getStateSize());

        // Equal states must have the same number of requests.
        int actualLength = actual.getBufferedRequestEntries().size();
        assertThat(actualLength).isEqualTo(expected.getBufferedRequestEntries().size());

        List<RequestEntryWrapper<T>> actualRequests = actual.getBufferedRequestEntries();
        List<RequestEntryWrapper<T>> expectedRequests = expected.getBufferedRequestEntries();

        // Equal states must have same requests in the same order.
        for (int i = 0; i < actualLength; i++) {
            assertThat(actualRequests.get(i).getRequestEntry())
                    .isEqualTo(expectedRequests.get(i).getRequestEntry());
            assertThat(actualRequests.get(i).getSize())
                    .isEqualTo(expectedRequests.get(i).getSize());
        }
    }
}
