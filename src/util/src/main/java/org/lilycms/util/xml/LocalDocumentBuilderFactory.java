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

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Since lookup of DocumentBuilderFactory is slow, and DocumentBuilderFactory
 * is not guaranteed to be thread-safe, this class exploits the technique of
 * using a thread local variable to cache a DocumentBuilderFactory instance.
 */
public class LocalDocumentBuilderFactory {
    private static ThreadLocal<DocumentBuilderFactory> LOCAL = new ThreadLocal<DocumentBuilderFactory>() {
        protected DocumentBuilderFactory initialValue() {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setValidating(false);
            return factory;
        }
    };

    public static DocumentBuilderFactory getFactory() {
        return LOCAL.get();
    }

    public static DocumentBuilder getBuilder() {
        try {
            return LOCAL.get().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public static Document newDocument() {
        try {
            return LOCAL.get().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}
