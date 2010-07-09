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

import java.io.IOException;

/**
 * Result of executing a query.
 */
public interface QueryResult {

    /**
     * Move to and return the next result.
     *
     * @return the identifier of the next matching query result, or null if the end is reached.
     */
    public byte[] next() throws IOException;

    /**
     * Retrieves data that was stored as part of the {@link IndexEntry} from the current index
     * entry (corresponding to the last {@link #next} call).
     */
    public byte[] getData(byte[] qualifier);

    public byte[] getData(String qualifier);

    public String getDataAsString(String qualifier);
}
