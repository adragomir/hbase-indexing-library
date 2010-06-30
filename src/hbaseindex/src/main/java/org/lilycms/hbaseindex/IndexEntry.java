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

import java.util.HashMap;
import java.util.Map;

/**
 * An entry to add to or remove from an Index.
 *
 * <p>This object can be simply instantiated yourself and passed to
 * {@link Index#addEntry} or {@link Index#removeEntry}.
 *
 * <p>The fields added to this entry should correspond in name
 * and type to the fields defined in the {@link IndexDefinition}.
 * This is not validated when adding the fields to this IndexEntry,
 * but only later on when passing it to a method of {@link Index}.
 *
 * <p>Missing fields will be interpreted as fields with a null value.
 */
public class IndexEntry {
    private Map<String, Object> values = new HashMap<String, Object>();

    public void addField(String name, Object value) {
        values.put(name, value);
    }

    public Object getValue(String name) {
        return values.get(name);
    }

    protected Map<String, Object> getValues() {
        return values;
    }
}
