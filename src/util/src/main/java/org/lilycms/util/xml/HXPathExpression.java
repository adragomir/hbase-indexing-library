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

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathConstants;
import java.util.ArrayList;
import java.util.List;

/**
 * HXPath stands for "helpful" XPath, a small wrapper around
 * XPathExpression providing some convenience.
 */
public class HXPathExpression {
    private final XPathExpression expr;

    public HXPathExpression(XPathExpression expr) {
        this.expr = expr;
    }

    public String evalAsString(Node node) throws XPathExpressionException {
        String result = expr.evaluate(node);
        if (result.length() == 0)
            return null;
        return result;
    }

    public Boolean evalAsBoolean(Node node) throws XPathExpressionException {
        String value = expr.evaluate(node);
        if (value.length() == 0)
            return null;
        return Boolean.valueOf(value);
    }

    public Integer evalAsInteger(Node node) throws XPathExpressionException {
        String value = expr.evaluate(node);
        if (value.length() == 0)
            return null;
        return Integer.valueOf(value);
    }

    public NodeList evalAsNodeList(Node node) throws XPathExpressionException {
        return (NodeList)expr.evaluate(node, XPathConstants.NODESET);
    }

    public List<Node> evalAsNativeNodeList(Node node) throws XPathExpressionException {
        NodeList list = (NodeList)expr.evaluate(node, XPathConstants.NODESET);
        List<Node> newList = new ArrayList<Node>(list.getLength());
        for (int i = 0; i < list.getLength(); i++) {
            newList.add(list.item(i));
        }
        return newList;
    }

    public List<Element> evalAsNativeElementList(Node node) throws XPathExpressionException {
        NodeList list = (NodeList)expr.evaluate(node, XPathConstants.NODESET);
        List<Element> newList = new ArrayList<Element>(list.getLength());
        for (int i = 0; i < list.getLength(); i++) {
            newList.add((Element)list.item(i));
        }
        return newList;
    }
}
