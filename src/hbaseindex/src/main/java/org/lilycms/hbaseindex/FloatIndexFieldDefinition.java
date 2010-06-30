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

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.node.ObjectNode;

/**
 * An IndexFieldDefinition for floats.
 *
 * <p>Note that since float representation is always approximative, it is
 * better suited for range queries than equals queries.
 */
public class FloatIndexFieldDefinition  extends IndexFieldDefinition {
    public FloatIndexFieldDefinition(String name) {
        super(name, IndexValueType.FLOAT);
    }

    public FloatIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.FLOAT, jsonObject);
    }

    @Override
    public final int getLength() {
        return Bytes.SIZEOF_FLOAT;
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value) {
        return toBytes(bytes, offset, value, true);
    }

    @Override
    public int toBytes(byte[] bytes, int offset, Object value, boolean fillFieldLength) {
        float floatVal = (Float)value;
        int nextOffset = Bytes.putFloat(bytes, offset, floatVal);

        // Alter the binary representation of the float such that when comparing
        // the binary representations, the floats compare the same as when they
        // would be compared as floats.

        // There's many information on IEEE floating point numbers on the net, here some I used:
        //  * http://docs.sun.com/source/806-3568/ncg_math.html
        //  * http://www.stereopsis.com/radix.html
        //  * http://www.swarthmore.edu/NatSci/echeeve1/Ref/BinaryMath/NumSys.html#posfrac

        // Basically the float format is [sign bit][exponent bits][mantissa bits],
        // with the more significant bits to the left. For positive numbers, the
        // sorting will be automatically OK, but to get them bigger than the negatives
        // we flip the sign bit. Negative numbers are in sign-magnitude format (in
        // contrast to the two's complement representation of integers), to get
        // bigger magnitudes to become smaller we simply flip both the exponent and
        // mantissa bits.

        // Handling of infinity, subnormal numbers and both zeros (pos & neg) and NaN
        // should be fine

        // Check the leftmost bit to determine if the value is negative
        int test = (bytes[offset] >>> 7) & 0x01;
        if (test == 1) {
            // Negative numbers: invert all bits: sign, exponent and mantissa
            for (int i = offset; i < getLength(); i++) {
                bytes[i] = (byte)(bytes[i] ^ 0xFF);
            }
        } else {
            // Positive numbers: invert the sign bit
            bytes[offset] = (byte)(bytes[offset] | 0x80);
        }

        return nextOffset;
    }
}

