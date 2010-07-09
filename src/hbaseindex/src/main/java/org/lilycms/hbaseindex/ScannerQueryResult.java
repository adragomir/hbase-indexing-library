/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * A QueryResult on top of a HBase scanner.
 */
class ScannerQueryResult extends BaseQueryResult {
    private ResultScanner scanner;
    private boolean invertIdentifier;

    public ScannerQueryResult(ResultScanner scanner, boolean invertIdentifier) {
        this.scanner = scanner;
        this.invertIdentifier = invertIdentifier;
    }

    public byte[] next() throws IOException {
        currentResult = scanner.next();
        if (currentResult == null) {
            return null;
        }

        byte[] rowKey = currentResult.getRow();

        byte[] identifier = IdentifierEncoding.decode(rowKey, invertIdentifier);
        
        return identifier;
    }
}
