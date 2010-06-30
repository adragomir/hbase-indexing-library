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

import java.math.BigDecimal;
import java.math.BigInteger;

public class DecimalIndexFieldDefinition extends IndexFieldDefinition {
    private int length = 10;

    private static final int EXP_OFFSET = (int)Math.pow(2, 14); // half of the largest number that can be stored in 15 bits

    public DecimalIndexFieldDefinition(String name) {
        super(name, IndexValueType.DECIMAL);
    }

    public DecimalIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.DECIMAL, jsonObject);

        if (jsonObject.get("length") != null)
            this.length = jsonObject.get("length").getIntValue();
    }

    @Override
    public int getLength() {
        return length;
    }

    /**
     * Maximum length used by this field. This should be minimum 3.
     *
     * <p>Two bytes are used for the sign and the exponent, the remainder for the
     * mantissa, as explained in {@link #toBytes}.
     */
    public void setLength(int length) {
        if (length < 3)
            throw new IllegalArgumentException("Minimum length for a decimal field should be 3 bytes, got: " + length);
        this.length = length;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    /**
     * Converts a decimal to sortable bytes.
     *
     * <p>The format is as follows:
     *
     * <pre>
     * [1 sign bit][15 bits unsigned exponent][variable number of mantissa bits]
     * </pre>
     *
     * <p>The exponent is unsigned: the exponents are augmented with an offset of 2^14 so that they
     * all shift into the positive number range. The value of the exponent is
     * BigDecimal.precision() - BigDecimal.scale(). The bits of the exponent are inverted if
     * the number (not the exponent) is negative.
     *
     * <p>The mantissa is two's complement as obtained by BigDecimal.unscaledValue().toByteArray().
     * This includes a sign bit which we don't really need, we invert it such that positive numbers
     * become larger than negative ones. The maximum length of the mantissa is determined by
     * {@link #setLength}.
     *
     */
    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        BigDecimal dec = (BigDecimal)value;

        BigInteger mantissa = dec.unscaledValue();
        int exp = dec.precision() - dec.scale();
        int signum = dec.signum();

        byte[] mantissaBytes = mantissa.toByteArray();
        // Mantissabytes are in two's complement, just invert the sign bit
        mantissaBytes[0] = (byte)(mantissaBytes[0] ^ 0x80);

        if ((exp > (EXP_OFFSET - 1)) || (exp < (-1 * EXP_OFFSET))) {
            throw new RuntimeException("Cannot convert number to sortable decimal: exp = " + exp + ", mantissa = [0.]" + mantissa);
        }

        // Similar to IEEE floating points, add an offset to the exponent so that it becomes 0-based.
        exp += EXP_OFFSET;

        // For negative numbers, flip the exponent
        if (signum < 0) {
            exp ^= 0xFF;
        }

        // Result array
        int resultByteSize = Math.min(mantissaBytes.length + 2, length);

        // Copy exponent into result array
        bytes[offset] = (byte)exp;
        exp >>>= 8;
        bytes[offset + 1] = (byte)exp;

        // Set the sign bit
        if (signum >= 0) {
            bytes[offset] = (byte)(bytes[offset] | 0x80);
        } else {
            bytes[offset] = (byte)(bytes[offset] & 0x7F);
        }

        // Copy mantissa into result array
        int mantissaLength = Math.min(mantissaBytes.length, length - 2);
        System.arraycopy(mantissaBytes, 0, bytes, offset + 2, mantissaLength);

        return offset + length;
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("length", length);
        return object;
    }
}
