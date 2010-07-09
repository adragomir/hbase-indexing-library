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

import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.util.ArgumentValidator;
import org.lilycms.util.LocaleHelper;

import java.io.UnsupportedEncodingException;
import java.text.Collator;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A string field in an index.
 *
 * <p>Strings can be of variable length, therefore the {#setLength} method returns -1.
 *
 * <p>Various options can be set for this field:
 *
 * <ul>
 *  <li>{@link #setByteEncodeMode}: the way a string should be mapped onto bytes: as utf8,
 *    by folding everything to ascii, or by using collation keys.
 *  <li>
 *  <li>{@link #setLocale}
 *  <li>{@link #setCaseSensitive}
 * </ul>
 */
public class StringIndexFieldDefinition extends IndexFieldDefinition {
    public enum ByteEncodeMode { UTF8, ASCII_FOLDING, COLLATOR }

    private Locale locale = Locale.US;
    private ByteEncodeMode byteEncodeMode = ByteEncodeMode.UTF8;
    private boolean caseSensitive = true;

    private static Map<ByteEncodeMode, StringEncoder> ENCODERS;
    static {
        ENCODERS = new HashMap<ByteEncodeMode, StringEncoder>();
        ENCODERS.put(ByteEncodeMode.UTF8, new Utf8StringEncoder());
        ENCODERS.put(ByteEncodeMode.ASCII_FOLDING, new AsciiFoldingStringEncoder());
        ENCODERS.put(ByteEncodeMode.COLLATOR, new CollatorStringEncoder());
    }

    /**
     * The end-of-field marker consists of all-zero-bits bytes. This is to achieve the effect
     * that shorter strings would always sort before those that contain an additional character.
     * Supposing a zero is an allowed byte within an encoded string, the further bytes should
     * again be zeros for the same purpose.
     *
     * <p>This makes the assumption that a sequence of zero bytes will not occur in an encoded
     * string (see also the toBytes method where this is checked, and where the same sequence is also hardcoded!)
     */
    private static final byte[] EOF_MARKER = new byte[] {0, 0, 0, 0};

    public StringIndexFieldDefinition(String name) {
        super(name, IndexValueType.STRING);
    }

    public StringIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.STRING, jsonObject);

        if (jsonObject.get("locale") != null)
            this.locale = LocaleHelper.parseLocale(jsonObject.get("locale").getTextValue());
        if (jsonObject.get("byteEncodeMode") != null)
            this.byteEncodeMode = ByteEncodeMode.valueOf(jsonObject.get("byteEncodeMode").getTextValue());
        if (jsonObject.get("caseSensitive") != null)
            this.caseSensitive = jsonObject.get("caseSensitive").getBooleanValue();
    }

    public Locale getLocale() {
        return locale;
    }

    /**
     * The Locale to use. This locale is used to fold the case when
     * case sensitivity is not desired, and is used in case the
     * {@link ByteEncodeMode#COLLATOR} mode is selected.
     */
    public void setLocale(Locale locale) {
        ArgumentValidator.notNull(locale, "locale");
        this.locale = locale;
    }

    public ByteEncodeMode getByteEncodeMode() {
        return byteEncodeMode;
    }

    /**
     * Sets the way the string should be converted to bytes. This will
     * determine how the items are ordered in the index (which is important
     * for range searches) and also whether searches are sensitive to accents
     * and the like.
     */
    public void setByteEncodeMode(ByteEncodeMode byteEncodeMode) {
        ArgumentValidator.notNull(byteEncodeMode, "byteEncodeMode");
        this.byteEncodeMode = byteEncodeMode;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Indicates if the string index should be case sensitive.
     */
    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public int getLength() {
        return -1;
    }

    @Override
    public byte[] getEndOfFieldMarker() {
        return EOF_MARKER;
    }

    @Override
    public byte[] toBytes(Object value) {
        String string = (String)value;
        string = Normalizer.normalize(string, Normalizer.Form.NFC);

        if (!caseSensitive) {
            string = string.toLowerCase(locale);
        }

        byte[] bytes = ENCODERS.get(byteEncodeMode).toBytes(string, locale);

        for (int i = 0; i <= bytes.length - 4; i++) {
            if (bytes[i] == 0 && bytes[i + 1] == 0 && bytes[i + 2] == 0 && bytes[i + 3] == 0) {
                // TODO what are the chances of this happening?
                // For most cases it does actually not matter if the EOF sequence would appear
                // in the encoded string, but for equals-string searches it matters.
                throw new RuntimeException("Encoded string value contains the end-of-field marker (zero byte).");
            }
        }
        return bytes;
    }

    private interface StringEncoder {
        byte[] toBytes(String string, Locale locale);
    }

    private static class Utf8StringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            try {
                return string.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class AsciiFoldingStringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            try {
                return ASCIIFoldingUtil.foldToASCII(string).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class CollatorStringEncoder implements StringEncoder {
        public byte[] toBytes(String string, Locale locale) {
            Collator collator = Collator.getInstance(locale);
            return collator.getCollationKey(string).toByteArray();
        }
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("locale", LocaleHelper.getString(locale));
        object.put("byteEncodeMode", byteEncodeMode.toString());
        object.put("caseSensitive", caseSensitive);
        return object;
    }
}
