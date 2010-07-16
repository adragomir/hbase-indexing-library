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

import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPath;

/**
 * Serves a similar purpose as {@link LocalDocumentBuilderFactory}.
 */
public class LocalXPathFactory {
    private static ThreadLocal<XPathFactory> LOCAL = new ThreadLocal<XPathFactory>() {
        protected XPathFactory initialValue() {
            XPathFactory factory = XPathFactory.newInstance();
            return factory;
        }
    };

    public static XPathFactory getFactory() {
        return LOCAL.get();
    }

    public static XPath newXPath() {
        return LOCAL.get().newXPath();
    }
}
