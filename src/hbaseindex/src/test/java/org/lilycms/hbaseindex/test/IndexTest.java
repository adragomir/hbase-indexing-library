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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.lilycms.hbaseindex.*;
import org.lilycms.testfw.TestHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

public class IndexTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);

        IndexManager.createIndexMetaTable(TEST_UTIL.getConfiguration());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testSingleStringFieldIndex() throws Exception {
        final String INDEX_NAME = "singleStringField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"d", "a", "c", "e", "b"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", "b", "d");
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key4", "key2", "key0");
    }

    @Test
    public void testSingleByteFieldIndex() throws Exception {
        final String INDEX_NAME = "singleByteField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        ByteIndexFieldDefinition fieldDef = indexDef.addByteField("field1");
        fieldDef.setLength(3);
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        byte[][] values = {Bytes.toBytes("aaa"), Bytes.toBytes("aab")};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", Bytes.toBytes("aaa"), Bytes.toBytes("aab"));
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key0", "key1");
    }

    @Test
    public void testSingleIntFieldIndex() throws Exception {
        final String INDEX_NAME = "singleIntField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        final int COUNT = 1000;
        final int MAXVALUE = Integer.MAX_VALUE;
        int[] values = new int[COUNT];

        for (int i = 0; i < COUNT; i++) {
            values[i] = (int)(Math.random() * MAXVALUE);
        }

        for (int value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("key" + value));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Integer(0), new Integer(MAXVALUE));
        QueryResult result = index.performQuery(query);

        Arrays.sort(values);

        for (int value : values) {
            assertEquals("key" + value, Bytes.toString(result.next()));
        }

        assertNull(result.next());
    }

    @Test
    public void testSingleLongFieldIndex() throws Exception {
        final String INDEX_NAME = "singleLongField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addLongField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        long values[] = {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE};
        for (long value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("key" + value));
        }

        Query query = new Query();
        query.setRangeCondition("field1", Long.MIN_VALUE, Long.MAX_VALUE);
        QueryResult result = index.performQuery(query);

        for (long value : values) {
            assertEquals("key" + value, Bytes.toString(result.next()));
        }

        assertNull(result.next());
    }

    @Test
    public void testSingleFloatFieldIndex() throws Exception {
        final String INDEX_NAME = "singleFloatField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addFloatField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        float[] values = {55.45f, 63.88f, 55.46f, 55.47f, -0.3f};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Float(55.44f), new Float(55.48f));
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key0", "key2", "key3");
    }

    @Test
    public void testSingleDateTimeFieldIndex() throws Exception {
        final String INDEX_NAME = "singleDateTimeField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        DateTimeIndexFieldDefinition fieldDef = indexDef.addDateTimeField("field1");
        fieldDef.setPrecision(DateTimeIndexFieldDefinition.Precision.DATETIME_NOMILLIS);
        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        Date[] values = {
                new GregorianCalendar(2010, 1, 15, 14, 5, 0).getTime(),
                new GregorianCalendar(2010, 1, 15, 14, 5, 1).getTime(),
                new GregorianCalendar(2010, 1, 16, 10, 0, 0).getTime(),
                new GregorianCalendar(2010, 1, 17, 10, 0, 0).getTime()
        };

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new GregorianCalendar(2010, 1, 15, 14, 5, 0).getTime(),
                new GregorianCalendar(2010, 1, 15, 14, 5, 1).getTime());
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key0", "key1");
    }

    @Test
    public void testSingleDecimalFieldIndex() throws Exception {
        final String INDEX_NAME = "singleDecimalField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addDecimalField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        String[] values = {"33.66", "-1", "-3.00007E77"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", new BigDecimal(values[i]));
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", new BigDecimal(values[2]), new BigDecimal(values[0]));
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key2", "key1", "key0");
        }

        {
            Query query = new Query();
            query.addEqualsCondition("field1", new BigDecimal(values[2]));
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key2");
        }
    }

    @Test
    public void testDuplicateValuesIndex() throws Exception {
        final String INDEX_NAME = "duplicateValues";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"a", "a", "a", "a", "b", "c", "d"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.addEqualsCondition("field1", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(4, result);
    }

    @Test
    public void testMultiFieldIndex() throws Exception {
        final String INDEX_NAME = "multiField";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexDef.addStringField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("key1"));
        index.addEntry(entry, Bytes.toBytes("key2"));
        index.addEntry(entry, Bytes.toBytes("key3"));

        entry = new IndexEntry();
        entry.addField("field1", 11);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("key4"));

        entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "b");
        index.addEntry(entry, Bytes.toBytes("key5"));

        Query query = new Query();
        query.addEqualsCondition("field1", 10);
        query.addEqualsCondition("field2", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(3, result);
    }

    @Test
    public void testDeleteFromIndex() throws Exception {
        final String INDEX_NAME = "deleteFromIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        // Add the entry
        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "foobar");
        index.addEntry(entry, Bytes.toBytes("key1"));

        // Test it is there
        Query query = new Query();
        query.addEqualsCondition("field1", "foobar");
        QueryResult result = index.performQuery(query);
        assertEquals("key1", Bytes.toString(result.next()));
        assertNull(result.next());

        // Delete the entry
        index.removeEntry(entry, Bytes.toBytes("key1"));

        // Test it is gone
        result = index.performQuery(query);
        assertNull(result.next());

        // Delete the entry again, this should not give an error
        index.removeEntry(entry, Bytes.toBytes("key1"));
    }

    @Test
    public void testNullIndex() throws Exception {
        final String INDEX_NAME = "nullIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addStringField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "foobar");
        index.addEntry(entry, Bytes.toBytes("key1"));

        entry = new IndexEntry();
        index.addEntry(entry, Bytes.toBytes("key2"));

        entry = new IndexEntry();
        entry.addField("field2", "foobar");
        index.addEntry(entry, Bytes.toBytes("key3"));

        Query query = new Query();
        query.addEqualsCondition("field1", "foobar");
        query.addEqualsCondition("field2", null);
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key1");

        query = new Query();
        query.addEqualsCondition("field1", null);
        query.addEqualsCondition("field2", null);
        result = index.performQuery(query);
        assertResultIds(result, "key2");

        query = new Query();
        query.addEqualsCondition("field1", null);
        query.addEqualsCondition("field2", "foobar");
        result = index.performQuery(query);
        assertResultIds(result, "key3");
    }

    @Test
    public void testNotExistingIndex() throws Exception {
        final String INDEX_NAME = "notExisting";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        try {
            indexManager.getIndex(INDEX_NAME, INDEX_NAME);
            fail("Expected an IndexNotFoundException.");
        } catch (IndexNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testDeleteIndex() throws Exception {
        final String INDEX_NAME = "deleteIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("foo");
        indexManager.createIndex(indexDef);

        indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        indexManager.deleteIndex(INDEX_NAME, INDEX_NAME);

        try {
            indexManager.getIndex(INDEX_NAME, INDEX_NAME);
            fail("Expected an IndexNotFoundException.");
        } catch (IndexNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testIndexEntryVerificationIndex() throws Exception {
        final String INDEX_NAME = "indexEntryVerification";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("stringfield");
        indexDef.addFloatField("floatfield");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("nonexistingfield", "foobar");

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }

        entry = new IndexEntry();
        entry.addField("stringfield", new Integer(55));

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }

        entry = new IndexEntry();
        entry.addField("floatfield", "hello world");

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }
    }

    @Test
    public void testStringPrefixQuery() throws Exception {
        final String INDEX_NAME = "stringPrefixQuery";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        String[] values = {"baard", "boer", "beek", "kanaal", "paard"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", "b", "b");
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key0", "key2", "key1");
    }

    /**
     * Test searching on a subset of the fields.
     */
    @Test
    public void testPartialQuery() throws Exception {
        final String INDEX_NAME = "partialQuery";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addIntegerField("field2");
        indexDef.addStringField("field3");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        for (int i = 0; i < 3; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", "value A " + i);
            entry.addField("field2", 10 + i);
            entry.addField("field3", "value B " + i);
            index.addEntry(entry, Bytes.toBytes("key" + i));
        }

        // Search only on the leftmost field
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key0");
        }

        // Search only on the two leftmost fields
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            query.addEqualsCondition("field2", 10);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key0");
        }

        // Search only on the two leftmost fields, with range query on the second
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            query.setRangeCondition("field2", 9, 11);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key0");
        }

        // Try searching on just the second field, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field2", 10);
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try searching on just the second field, should give error
        {
            Query query = new Query();
            query.setRangeCondition("field2", 9, 11);
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try not using all fields from left to right, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            // skip field 2
            query.addEqualsCondition("field3", "value B 0");
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try not using all fields from left to right, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            // skip field 2
            query.setRangeCondition("field3", "a", "b");
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }
    }

    @Test
    public void testDataTypeChecks() throws Exception {
        final String INDEX_NAME = "dataTypeChecks";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addIntegerField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        //
        // Index entry checks
        //

        // First test correct situation
        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "a");
        index.addEntry(entry, Bytes.toBytes("1"));

        // Now test incorrect situation
        entry = new IndexEntry();
        entry.addField("field1", 55);
        try {
            index.addEntry(entry, Bytes.toBytes("1"));
            fail("Expected exception.");
        } catch (MalformedIndexEntryException e) {
            //System.out.println(e.getMessage());
        }

        //
        // Query checks
        //

        // First test correct situation
        Query query = new Query();
        query.addEqualsCondition("field1", "a");
        index.performQuery(query);

        // Now test incorrect situation
        query = new Query();
        query.addEqualsCondition("field1", 55);
        try {
            index.performQuery(query);
            fail("Expected exception.");
        } catch (MalformedQueryException e) {
            //System.out.println(e.getMessage());
        }
    }

    @Test
    public void testEmptyDefinition() throws Exception {
        final String INDEX_NAME = "emptyDef";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        try {
            indexManager.createIndex(indexDef);
            fail("Exception expected.");
        } catch (IllegalArgumentException e) {}
    }    

    @Test
    public void testExclusiveRanges() throws Exception {
        final String INDEX_NAME = "exclusiveRanges";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        int[] values = {1, 2, 3, 4};
        for (int value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("key" + value));
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 1, 4);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key1", "key2", "key3", "key4");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 1, 4, false, false);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key2", "key3");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 1, 4, false, true);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key2", "key3", "key4");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 1, 4, true, false);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key1", "key2", "key3");
        }
    }

    @Test
    public void testMinMaxRanges() throws Exception {
        final String INDEX_NAME = "minmaxranges";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        Integer[] values = {Integer.MIN_VALUE, 1, 2, Integer.MAX_VALUE, null};
        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + (i + 1)));
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", Query.MIN_VALUE, Query.MAX_VALUE);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key1", "key2", "key3", "key4", "key5");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", Query.MIN_VALUE, 0);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key1");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 0, Query.MAX_VALUE);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key2", "key3", "key4", "key5");
        }
    }

    @Test
    public void testDescendingIntIndex() throws Exception {
        final String INDEX_NAME = "descendingIntIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.setIdentifierOrder(Order.DESCENDING);
        IntegerIndexFieldDefinition fieldDef = indexDef.addIntegerField("field1");
        fieldDef.setOrder(Order.DESCENDING);
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        Integer[] values = {1, 2, 2, 3, null};
        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + (i + 1)));
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", Query.MIN_VALUE, Query.MAX_VALUE);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key5", "key4", "key3", "key2", "key1");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", 3, 1);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key4", "key3", "key2", "key1");
        }
    }

    @Test
    public void testDescendingIntAscendingKeyIndex() throws Exception {
        final String INDEX_NAME = "descendingIntAscendingKey";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        IntegerIndexFieldDefinition fieldDef = indexDef.addIntegerField("field1");
        fieldDef.setOrder(Order.DESCENDING);
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        Integer[] values = {1, 1, 2, 2};
        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + (i + 1)));
        }

        // The index on the value is descending, the identifiers themselves are ascending!

        Query query = new Query();
        query.setRangeCondition("field1", 2, 1);
        QueryResult result = index.performQuery(query);
        assertResultIds(result, "key3", "key4", "key1", "key2");
    }

    @Test
    public void testDescendingStringIndex() throws Exception {
        final String INDEX_NAME = "descendingStringIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.setIdentifierOrder(Order.DESCENDING);
        StringIndexFieldDefinition fieldDef = indexDef.addStringField("field1");
        fieldDef.setOrder(Order.DESCENDING);
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        String[] values = {"a", "ab", "abc", "b"};
        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("key" + (i + 1)));
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", Query.MIN_VALUE, Query.MAX_VALUE);
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key4", "key3", "key2", "key1");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", "b", "a");
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key4", "key3", "key2", "key1");
        }

        {
            Query query = new Query();
            query.setRangeCondition("field1", "a", "a");
            QueryResult result = index.performQuery(query);
            assertResultIds(result, "key3", "key2", "key1");
        }
    }

    @Test
    public void testData() throws Exception {
        final String INDEX_NAME = "dataIndex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME, INDEX_NAME);
        indexDef.addStringField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME, INDEX_NAME);

        String[] values = new String[] {"foo", "bar"};

        for (String value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            entry.addData(Bytes.toBytes("originalValue"), Bytes.toBytes(value));
            index.addEntry(entry, Bytes.toBytes(value));
        }


        Query query = new Query();
        query.setRangeCondition("field1", Query.MIN_VALUE, Query.MAX_VALUE);
        QueryResult result = index.performQuery(query);

        assertNotNull(result.next());
        assertEquals("bar", result.getDataAsString("originalValue"));

        assertNotNull(result.next());
        assertEquals("foo", result.getDataAsString("originalValue"));
    }

    private void assertResultIds(QueryResult result, String... identifiers) throws IOException {
        int i = 0;
        byte[] identifier;
        while ((identifier = result.next()) != null) {
            if (i >= identifiers.length) {
                fail("Too many query results.");
            }
            assertEquals(identifiers[i], Bytes.toString(identifier));
            i++;
        }
        assertNull(result.next());
    }

    private void assertResultSize(int expectedCount, QueryResult result) throws IOException {
        int matchCount = 0;
        while (result.next() != null) {
            matchCount++;
        }
        assertEquals(expectedCount, matchCount);
    }
}

