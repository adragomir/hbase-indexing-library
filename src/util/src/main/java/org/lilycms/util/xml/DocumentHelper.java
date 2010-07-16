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

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.lilycms.util.location.LocationAttributes;
import org.lilycms.util.location.LocatedException;
import org.lilycms.util.location.LocatedRuntimeException;
import org.lilycms.util.io.IOUtils;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.TransformerConfigurationException;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;

/**
 * Utility methods for working with DOM documents.
 */
public class DocumentHelper {
    public static Document parse(InputStream is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        Document document = factory.newDocumentBuilder().parse(is);
        return document;
    }

    public static Document parse(File file) throws ParserConfigurationException, IOException, SAXException {
        InputStream is = null;
        try {
            is = new FileInputStream(file);
            return parse(is);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public static Element getElementChild(Element element, String name, boolean required) throws Exception {
        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Element && node.getLocalName().equals(name) && node.getNamespaceURI() == null) {
                return (Element)node;
            }
        }
        if (required)
            throw new LocatedException("Missing element " + name + " in " + element.getLocalName(), LocationAttributes.getLocation(element));
        else
            return null;
    }

    public static Element[] getElementChildren(Element element) {
        List<Element> elements = new ArrayList<Element>();
        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            if (nodes.item(i) instanceof Element) {
                elements.add((Element)nodes.item(i));
            }
        }
        return elements.toArray(new Element[0]);
    }

    public static Element[] getElementChildren(Element element, String name) {
        Element[] elements = getElementChildren(element);
        List<Element> taggedElements = new ArrayList<Element>();
        for (int i = 0; i < elements.length; i++) {
            if (elements[i].getLocalName().equals(name) && elements[i].getNamespaceURI() == null) {
                taggedElements.add(elements[i]);
            }
        }
        return taggedElements.toArray(new Element[0]);
    }

    public static String getAttribute(Element element, String name, boolean required) throws Exception {
        if (!element.hasAttribute(name)) {
            if (required)
                throw new LocatedException("Missing attribute " + name + " on element " + element.getLocalName(), LocationAttributes.getLocation(element));
            else
                return null;
        }

        return element.getAttribute(name);
    }

    public static boolean getBooleanAttribute(Element element, String name, boolean defaultValue) throws Exception {
        String value = element.getAttribute(name);
        if (value.equals(""))
            return defaultValue;
        else
            return value.equalsIgnoreCase("true");
    }

    public static int getIntegerAttribute(Element element, String name) throws Exception {
        if (!element.hasAttribute(name)) {
            throw new LocatedException("Missing attribute " + name + " on element " + element.getLocalName(), LocationAttributes.getLocation(element));
        } else {
            String value = element.getAttribute(name);
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new LocatedException("Invalid integer value " + value + " in attribute " + name, LocationAttributes.getLocation(element));
            }
        }
    }

    public static int getIntegerAttribute(Element element, String name, int defaultValue) throws Exception {
        String value = element.getAttribute(name);
        if (value.equals("")) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new LocatedException("Invalid integer value " + value + " in attribute " + name, LocationAttributes.getLocation(element));
            }
        }
    }

    public static boolean getBooleanElement(Element element, String name, boolean defaultValue) throws Exception {
        Element child = getElementChild(element, name, false);
        if (child == null)
            return defaultValue;

        return getElementText(child, true).equalsIgnoreCase("true");
    }

    public static String getStringElement(Element element, String name, String defaultValue) throws Exception {
        Element child = getElementChild(element, name, false);
        if (child == null)
            return defaultValue;

        return getElementText(child, true);
    }

    public static int getIntegerElement(Element element, String name, int defaultValue) throws Exception {
        Element child = getElementChild(element, name, false);
        if (child == null)
            return defaultValue;

        String text = getElementText(child, true);
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new LocatedRuntimeException("Invalid integer value " + text + " in element " + name, LocationAttributes.getLocation(element));
        }
    }

    public static String getElementText(Element element, boolean required) throws Exception {
        StringBuilder text = new StringBuilder(30);
        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Text)
                text.append(node.getNodeValue());
        }

        if (required && text.length() == 0)
            throw new LocatedException("Missing text content in element " + element.getLocalName(), LocationAttributes.getLocation(element));
        else if (text.length() == 0)
            return null;
        else
            return text.toString();
    }

    public static Document parseDomWithLocationAttributes(File file) throws SAXException, ParserConfigurationException, TransformerConfigurationException, IOException {
        InputStream is = null;
        try {
            is = new FileInputStream(file);
            InputSource inputSource = new InputSource(is);
            inputSource.setSystemId(file.getCanonicalPath());
            return parseDomWithLocationAttributes(inputSource);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    public static Document parseDomWithLocationAttributes(InputSource inputSource) throws SAXException, ParserConfigurationException, TransformerConfigurationException, IOException {
        XMLReader xmlReader = LocalSAXParserFactory.newXmlReader();
        TransformerHandler serializer = LocalTransformerFactory.get().newTransformerHandler();
        DOMResult result = new DOMResult();
        serializer.setResult(result);
        LocationAttributes.Pipe locationPipe = new LocationAttributes.Pipe(serializer);
        xmlReader.setContentHandler(locationPipe);
        xmlReader.parse(inputSource);
        return (Document)result.getNode();
    }
}