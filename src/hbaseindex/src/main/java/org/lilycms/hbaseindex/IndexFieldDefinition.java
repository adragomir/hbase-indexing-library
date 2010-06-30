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

import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.util.ArgumentValidator;

/**
 * Defines a field that is part of an {@link IndexDefinition}.
 */
public abstract class IndexFieldDefinition {
    private final String name;
    private Order order;
    private IndexValueType type;

    public IndexFieldDefinition(String name, IndexValueType type) {
        this(name, type, Order.ASCENDING);
    }

    public IndexFieldDefinition(String name, IndexValueType type, Order order) {
        this.name = name;
        this.order = order;
        this.type = type;
    }

    public IndexFieldDefinition(String name, IndexValueType type, ObjectNode jsonObject) {
        this(name, type);

        if (jsonObject.get("order") != null)
            this.order = Order.valueOf(jsonObject.get("order").getTextValue());
    }

    public String getName() {
        return name;
    }

    public IndexValueType getType() {
        return type;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        ArgumentValidator.notNull(order, "order");
        this.order = order;
    }

    /**
     * The length of this index field in bytes, thus the number of bytes
     * this entry needs in the index row key.
     */
    public abstract int getLength();

    /**
     * Same as the other toBytes method, with fillFieldLength = true.
     */
    public abstract int toBytes(byte[] bytes, int offset, Object value);

    /**
     * Converts the specified value to bytes according to the rules of this
     * IndexFieldDefinition.
     *
     * @param bytes the byte array into which the bytes should be added. The byte array
     *              should be large enough to store {@link #getLength()} bytes after the
     *              offset.
     * @param offset the offset at which the bytes should be added
     * @param value the value, assumed to be of the correct type
     * @param fillFieldLength if true, the bytes will be padded up to {@link #getLength ()},
     *                        and the returned offset will hence be located after this length.
     *                        If false, the returned offset will only be after the actual
     *                        value length. Note that data types like number always use the
     *                        same length, this is mainly intended for strings.
     * @return the offset after the written data, thus where the next data could be written
     */
    public abstract int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength);

    public ObjectNode toJson() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();
        object.put("class", this.getClass().getName());
        object.put("order", this.order.toString());
        return object;
    }
}
