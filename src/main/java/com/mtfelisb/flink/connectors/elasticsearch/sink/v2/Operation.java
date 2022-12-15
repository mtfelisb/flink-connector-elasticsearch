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

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;

import java.io.*;
import java.util.Objects;

public class Operation implements Serializable {
    private final BulkOperation bulkOperation;

    public Operation(BulkOperationVariant bulkOperationVariant) {
        this.bulkOperation = bulkOperationVariant._toBulkOperation();
    }

    public BulkOperation getBulkOperation() {
        return bulkOperation;
    }

    @Override
    public String toString() {
        return "Operation{" +
            "bulkOperation=" + bulkOperation._kind() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return Objects.equals(bulkOperation._kind(), operation.bulkOperation._kind());
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkOperation);
    }
}
