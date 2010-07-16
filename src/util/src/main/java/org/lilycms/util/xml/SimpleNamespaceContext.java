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
package org.lilycms.util.xml;

import javax.xml.namespace.NamespaceContext;
import javax.xml.XMLConstants;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

public class SimpleNamespaceContext implements NamespaceContext {
    private Map<String, String> prefixToUri = new HashMap<String, String>();

    public void addPrefix(String prefix, String uri) {
        prefixToUri.put(prefix, uri);
    }

    public String getNamespaceURI(String prefix) {
        if (prefix == null)
            throw new IllegalArgumentException("NUll argument: prefix");

        if (prefix.equals(XMLConstants.XML_NS_PREFIX))
            return XMLConstants.XML_NS_URI;
        else if (prefix.equals(XMLConstants.XMLNS_ATTRIBUTE))
            return XMLConstants.XMLNS_ATTRIBUTE_NS_URI;

        String uri = prefixToUri.get(prefix);
        if (uri != null)
            return uri;
        else
            return XMLConstants.NULL_NS_URI;
    }

    public String getPrefix(String namespaceURI) {
        throw new RuntimeException("Not implemented.");
    }

    public Iterator getPrefixes(String namespaceURI) {
        throw new RuntimeException("Not implemented.");
    }
}

