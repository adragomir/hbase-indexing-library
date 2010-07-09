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
import org.codehaus.jackson.node.ObjectNode;

public class IntegerIndexFieldDefinition extends IndexFieldDefinition {
    public IntegerIndexFieldDefinition(String name) {
        super(name, IndexValueType.INTEGER);
    }

    public IntegerIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.INTEGER, jsonObject);
    }

    @Override
    public int getLength() {
        return Bytes.SIZEOF_INT;
    }

    @Override
    public byte[] toBytes(Object value) {
        byte[] bytes = new byte[getLength()];
        int integer = (Integer)value;
        Bytes.putInt(bytes, 0, integer);

        // To make the integers sort correctly when comparing their binary
        // representations, we need to invert the sign bit
        bytes[0] ^= 0x80;

        return bytes;
    }
}
