package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Routines related to how the identifier (of the indexed item) is encoded in the index row key.
 *
 * <p>The encoding should be such that the identifier can be extracted from the row key, without
 * knowing what the length of the identifier or the part before that is. We achieve this by
 * appending the length of the key to the end of the key. This encoding also has the benefit
 * that it does not alter the identifier itself, so that the byte-ordering of it will stay the
 * same.
 */
public class IdentifierEncoding {
    public static byte[] encode(byte[] bytes) {
        byte[] result = new byte[bytes.length + Bytes.SIZEOF_INT];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        Bytes.putInt(result, bytes.length, bytes.length);
        return result;
    }

    /**
     * Extracts the identifier from an index row key.
     *
     * @param bytes byte array containing an encoded row key at its end (and arbitrary bytes before that).
     *              Note that this method modifies the bytes in case inverted is true!
     * @param inverted indicates if the bits in the row key are inverted (can be the case for descending ordering)
     */
    public static byte[] decode(byte[] bytes, boolean inverted) {
        if (inverted) {
            for (int i = 0; i < Bytes.SIZEOF_INT; i++) {
                int pos = bytes.length - i - 1;
                bytes[pos] = bytes[pos] ^= 0xFF;
            }
        }

        int keyLength = Bytes.toInt(bytes, bytes.length - Bytes.SIZEOF_INT);
        byte[] result = new byte[keyLength];
        System.arraycopy(bytes, bytes.length - keyLength - Bytes.SIZEOF_INT, result, 0, keyLength);
        
        if (inverted) {
            for (int j = 0; j < result.length; j++) {
                result[j] ^= 0xFF;
            }
        }

        return result;
    }
}
