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

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * A QueryResult which is the disjunction (= OR operation) of two other QueryResults.
 *
 * <p>The supplied QueryResults should adhere to the same requirements as for
 * {@link Conjunction}s. 
 */
public class Disjunction implements QueryResult {
    private QueryResult result1;
    private QueryResult result2;
    private byte[] key1;
    private byte[] key2;
    private boolean init = false;

    public Disjunction(QueryResult result1, QueryResult result2) {
        this.result1 = result1;
        this.result2 = result2;
    }

    public byte[] next() throws IOException {
        if (!init) {
            key1 = result1.next();
            key2 = result2.next();
            init = true;
        }

        if (key1 == null && key2 == null) {
            return null;
        } else if (key1 == null) {
            byte[] result = key2;
            key2 = result2.next();
            return result;
        } else if (key2 == null) {
            byte[] result = key1;
            key1 = result1.next();
            return result;
        }

        int cmp = Bytes.compareTo(key1, key2);

        if (cmp == 0) {
            byte[] result = key1;
            key1 = result1.next();
            key2 = result2.next();
            return result;
        } else if (cmp < 0) {
            byte[] result = key1;
            key1 = result1.next();
            return result;
        } else { // cmp > 0
            byte[] result = key2;
            key2 = result2.next();
            return result;
        }
    }
}
