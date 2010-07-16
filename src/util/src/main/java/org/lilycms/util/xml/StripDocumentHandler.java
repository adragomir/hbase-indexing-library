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

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.Locator;
import org.xml.sax.Attributes;
import org.xml.sax.ext.LexicalHandler;

/**
 * Removes startDocument and endDocument events from the SAX stream.
 */
public class StripDocumentHandler implements ContentHandler, LexicalHandler {
    private final ContentHandler ch;
    private final LexicalHandler lh;

    public StripDocumentHandler(ContentHandler contentHandler) {
        this(contentHandler, null);
    }

    public StripDocumentHandler(ContentHandler contentHandler, LexicalHandler lexicalHandler) {
        this.ch = contentHandler;
        this.lh = lexicalHandler;
    }

    public void endDocument() throws SAXException {
        // intentionally empty
    }

    public void startDocument () throws SAXException {
        // intentionally empty
    }

    public void characters (char ch[], int start, int length) throws SAXException {
        this.ch.characters(ch, start, length);
    }

    public void ignorableWhitespace (char ch[], int start, int length) throws SAXException {
        this.ch.ignorableWhitespace(ch, start, length);
    }

    public void endPrefixMapping (String prefix) throws SAXException {
        ch.endPrefixMapping(prefix);
    }

    public void skippedEntity (String name) throws SAXException {
        ch.skippedEntity(name);
    }

    public void setDocumentLocator (Locator locator) {
        ch.setDocumentLocator(locator);
    }

    public void processingInstruction (String target, String data) throws SAXException {
        ch.processingInstruction(target, data);
    }

    public void startPrefixMapping (String prefix, String uri) throws SAXException {
        ch.startPrefixMapping(prefix, uri);
    }

    public void endElement (String namespaceURI, String localName, String qName) throws SAXException {
        ch.endElement(namespaceURI, localName, qName);
    }

    public void startElement (String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
        ch.startElement(namespaceURI, localName, qName, atts);
    }

    public void startDTD(String name, String publicId, String systemId) throws SAXException {
    }

    public void endDTD() throws SAXException {
    }

    public void startEntity(String name) throws SAXException {
    }

    public void endEntity(String name) throws SAXException {
    }

    public void startCDATA() throws SAXException {
        if (lh != null)
            lh.startCDATA();
    }

    public void endCDATA() throws SAXException {
        if (lh != null)
            lh.endCDATA();
    }

    public void comment(char ch[], int start, int length) throws SAXException {
        if (lh != null)
            lh.comment(ch, start, length);
    }
}
