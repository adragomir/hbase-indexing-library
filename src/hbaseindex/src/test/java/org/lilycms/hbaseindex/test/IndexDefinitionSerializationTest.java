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

import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.lilycms.hbaseindex.*;

import static org.junit.Assert.*;

public class IndexDefinitionSerializationTest {
    @Test
    public void testStringField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index", "index");
        StringIndexFieldDefinition field = indexDef.addStringField("stringfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", "index", json);
        StringIndexFieldDefinition newField = (StringIndexFieldDefinition)newIndexDef.getField("stringfield");

        assertEquals(field.getName(), newField.getName());
        assertEquals(field.isCaseSensitive(), newField.isCaseSensitive());
        assertEquals(field.getByteEncodeMode(), newField.getByteEncodeMode());
        assertEquals(field.getLocale(), newField.getLocale());
    }

    @Test
    public void testIntegerField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index", "index");
        IntegerIndexFieldDefinition field = indexDef.addIntegerField("intfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", "index", json);
        IntegerIndexFieldDefinition newField = (IntegerIndexFieldDefinition)newIndexDef.getField("intfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testFloatField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index", "index");
        FloatIndexFieldDefinition field = indexDef.addFloatField("floatfield");
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", "index", json);
        FloatIndexFieldDefinition newField = (FloatIndexFieldDefinition)newIndexDef.getField("floatfield");

        assertEquals(field.getName(), newField.getName());
    }

    @Test
    public void testDateTimeField() throws Exception {
        IndexDefinition indexDef = new IndexDefinition("index", "index");
        DateTimeIndexFieldDefinition field = indexDef.addDateTimeField("datetimefield");
        field.setPrecision(DateTimeIndexFieldDefinition.Precision.DATETIME);
        ObjectNode json = indexDef.toJson();

        IndexDefinition newIndexDef = new IndexDefinition("index", "index", json);
        DateTimeIndexFieldDefinition newField = (DateTimeIndexFieldDefinition)newIndexDef.getField("datetimefield");

        assertEquals(field.getName(), newField.getName());
        assertEquals(field.getPrecision(), newField.getPrecision());
    }
}
