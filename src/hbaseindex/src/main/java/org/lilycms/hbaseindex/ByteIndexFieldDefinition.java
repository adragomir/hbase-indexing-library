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

import org.codehaus.jackson.node.ObjectNode;

/**
 * This kind of field allows to store arbitrary bytes (a byte array)
 * in the index key, the ideal fallback the case none of the other
 * types suite your needs.
 */
public class ByteIndexFieldDefinition extends IndexFieldDefinition {
    private int length = 10;

    public ByteIndexFieldDefinition(String name) {
        super(name, IndexValueType.BYTES);
    }

    public ByteIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.BYTES, jsonObject);

        if (jsonObject.get("length") != null)
            this.length = jsonObject.get("length").getIntValue();
    }

    /**
     * Set the length of this field, thus the maximum size of byte array
     * you want to store in it.
     *
     * <p>The default is 10.
     */
    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        byte[] byteValue = (byte[])value;


        int copyLength = byteValue.length < length ? byteValue.length : length;
        System.arraycopy(byteValue, 0, bytes, offset, copyLength);

        return fillFieldLength ? offset + length : offset + copyLength;
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("length", length);
        return object;
    }

}
