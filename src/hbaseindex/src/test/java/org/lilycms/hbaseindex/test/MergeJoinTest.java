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
package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.lilycms.hbaseindex.Conjunction;
import org.lilycms.hbaseindex.Disjunction;
import org.lilycms.hbaseindex.QueryResult;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class MergeJoinTest {
    @Test
    public void testConjunction() throws Exception {
        String[] values1 = {"a", "b", "c",           "f", "g"};
        String[] values2 = {     "b", "c", "d", "e", "f"};

        QueryResult result = new Conjunction(buildQueryResult(values1), buildQueryResult(values2));

        assertEquals("b", Bytes.toString(result.next()));
        assertEquals("c", Bytes.toString(result.next()));
        assertEquals("f", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testDisjunction() throws Exception {
        String[] values1 = {"a", "b", "c",           "f", "g"};
        String[] values2 = {     "b", "c", "d", "e", "f"};

        QueryResult result = new Disjunction(buildQueryResult(values1), buildQueryResult(values2));

        assertEquals("a", Bytes.toString(result.next()));
        assertEquals("b", Bytes.toString(result.next()));
        assertEquals("c", Bytes.toString(result.next()));
        assertEquals("d", Bytes.toString(result.next()));
        assertEquals("e", Bytes.toString(result.next()));
        assertEquals("f", Bytes.toString(result.next()));
        assertEquals("g", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    private QueryResult buildQueryResult(String[] values) {
        List<byte[]> byteValues = new ArrayList<byte[]>(values.length);

        for (String value : values) {
            byteValues.add(Bytes.toBytes(value));
        }

        return new StaticQueryResult(byteValues);
    }
}
