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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.util.ArgumentValidator;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Defines the structure of an index.
 *
 * <p>An index is defined by instantiating an object of this class, adding one
 * or more fields to it using the methods like {@link #addStringField},
 * {@link #addIntegerField}, etc. Finally the index is created by calling
 * {@link IndexManager#createIndex}. After creation, the definition of an index
 * cannot be modified.
 */
public class IndexDefinition {
    private String name;
    private List<IndexFieldDefinition> fields = new ArrayList<IndexFieldDefinition>();
    private Map<String, IndexFieldDefinition> fieldsByName = new HashMap<String, IndexFieldDefinition>();
    private Order identifierOrder = Order.ASCENDING;

    public IndexDefinition(String name) {
        ArgumentValidator.notNull(name, "name");
        this.name = name;
    }

    public IndexDefinition(String name, ObjectNode jsonObject) {
        this.name = name;

        if (jsonObject.get("identifierOrder") != null)
            identifierOrder = Order.valueOf(jsonObject.get("identifierOrder").getTextValue());

        try {
            ObjectNode fields = (ObjectNode)jsonObject.get("fields");
            Iterator<Map.Entry<String, JsonNode>> fieldsIt = fields.getFields();
            while (fieldsIt.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String className = entry.getValue().get("class").getTextValue();
                Class<IndexFieldDefinition> clazz = (Class<IndexFieldDefinition>)getClass().getClassLoader().loadClass(className); 
                Constructor<IndexFieldDefinition> constructor = clazz.getConstructor(String.class, ObjectNode.class);
                IndexFieldDefinition field = constructor.newInstance(entry.getKey(), entry.getValue());
                add(field);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error instantiating IndexDefinition.", e);
        }
    }

    public String getName() {
        return name;
    }

    public Order getIdentifierOrder() {
        return identifierOrder;
    }

    public void setIdentifierOrder(Order identifierOrder) {
        ArgumentValidator.notNull(identifierOrder, "identifierOrder");
        this.identifierOrder = identifierOrder;
    }

    public IndexFieldDefinition getField(String name) {
        return fieldsByName.get(name);
    }

    public StringIndexFieldDefinition addStringField(String name) {
        validateName(name);
        StringIndexFieldDefinition definition = new StringIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public IntegerIndexFieldDefinition addIntegerField(String name) {
        validateName(name);
        IntegerIndexFieldDefinition definition = new IntegerIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public FloatIndexFieldDefinition addFloatField(String name) {
        validateName(name);
        FloatIndexFieldDefinition definition = new FloatIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public DateTimeIndexFieldDefinition addDateTimeField(String name) {
        validateName(name);
        DateTimeIndexFieldDefinition definition = new DateTimeIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public DecimalIndexFieldDefinition addDecimalField(String name) {
        validateName(name);
        DecimalIndexFieldDefinition definition = new DecimalIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public LongIndexFieldDefinition addLongField(String name) {
        validateName(name);
        LongIndexFieldDefinition definition = new LongIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    public ByteIndexFieldDefinition addByteField(String name) {
        validateName(name);
        ByteIndexFieldDefinition definition = new ByteIndexFieldDefinition(name);
        add(definition);
        return definition;
    }

    private void add(IndexFieldDefinition fieldDef) {
        fields.add(fieldDef);
        fieldsByName.put(fieldDef.getName(), fieldDef);
    }

    private void validateName(String name) {
        ArgumentValidator.notNull(name, "name");
        if (fieldsByName.containsKey(name)) {
            throw new IllegalArgumentException("Field name already exists in this IndexDefinition: " + name);
        }
    }

    public List<IndexFieldDefinition> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public ObjectNode toJson() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode object = factory.objectNode();
        ObjectNode fieldsJson = object.putObject("fields");

        for (IndexFieldDefinition field : fields) {
            fieldsJson.put(field.getName(), field.toJson());
        }

        object.put("identifierOrder", identifierOrder.toString());

        return object;
    }

}
