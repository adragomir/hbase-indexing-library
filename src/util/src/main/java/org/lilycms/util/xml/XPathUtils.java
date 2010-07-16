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

import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

/**
 * Convenience XPath methods, intended for occassional use or cases
 * where performance is not an issue (e.g. in testcases), as
 * no compiled expression caching is involved.
 *
 * <p>See {@link LocalXPathExpression} for heavy-duty usages.
 */
public class XPathUtils {
    public static NodeList evalNodeList(String expression, Node node) {
        try {
            return (NodeList)LocalXPathFactory.newXPath().evaluate(expression, node, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static Element evalElement(String expression, Node node) {
        try {
            Node result = (Node)LocalXPathFactory.newXPath().evaluate(expression, node, XPathConstants.NODE);
            if (!(result instanceof Element))
                throw new RuntimeException("Expected an element from the evaluation of the xpath expression " + expression + ", but got a " + result.getClass().getName());
            return (Element)result;
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static String evalString(String expression, Node node) {
        try {
            return (String)LocalXPathFactory.newXPath().evaluate(expression, node, XPathConstants.STRING);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static int evalInt(String expression, Node node) {
        try {
            Number result = (Number)LocalXPathFactory.newXPath().evaluate(expression, node, XPathConstants.NUMBER);
            return result.intValue();
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }
}
