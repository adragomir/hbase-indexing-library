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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.util.ArgumentValidator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Allows to query an index, and add entries to it or remove entries from it.
 *
 * <p>An Index instance can be obtained from {@link IndexManager#getIndex}.
 *
 * <p>The Index class <b>is not thread safe</b> for writes, because the underlying
 * HBase HTable is not thread safe for writes.
 *
 */
public class Index {
  private HTable htable;
  private IndexDefinition definition;

  protected static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  private static final byte[] DUMMY_QUALIFIER = Bytes.toBytes("dummy");
  private static final byte[] DUMMY_VALUE = Bytes.toBytes("dummy");

  /** Number of bytes overhead per field. */
  private static final int FIELD_FLAGS_SIZE = 1;

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  protected Index(HTable htable, IndexDefinition definition) {
    this.htable = htable;
    this.definition = definition;
  }

  /**
   * Adds an entry to this index. See {@link IndexEntry} for more information.
   *
   * @param entry the values to be part of the index key, should correspond to the fields
   *              defined in the {@link IndexDefinition}
   * @param identifier the identifier of the indexed object, typically the key of a row in
   *                   another HBase table
   */
  public void addEntry(IndexEntry entry, byte[] identifier) throws IOException {
    ArgumentValidator.notNull(entry, "entry");
    ArgumentValidator.notNull(identifier, "identifier");
    validateIndexEntry(entry);

    byte[] indexKey = buildRowKey(entry, identifier);
    Put put = new Put(indexKey);

    Map<IndexEntry.ByteArrayKey, byte[]> data = entry.getData();
    if (data.size() > 0) {
      for (Map.Entry<IndexEntry.ByteArrayKey, byte[]> item : data.entrySet()) {
        put.add(DATA_FAMILY, item.getKey().getKey(), item.getValue());
      }
    } else {
      // HBase does not allow to create a row without columns, so add a dummy column
      put.add(DATA_FAMILY, DUMMY_QUALIFIER, DUMMY_VALUE);
    }

    htable.put(put);
  }

  /**
   * Removes an entry from the index. The contents of the supplied
   * entry and the identifier should exactly match those supplied
   * when creating the index entry.
   */
  public void removeEntry(IndexEntry entry, byte[] identifier) throws IOException {
    ArgumentValidator.notNull(entry, "entry");
    ArgumentValidator.notNull(identifier, "identifier");
    validateIndexEntry(entry);

    byte[] indexKey = buildRowKey(entry, identifier);
    Delete delete = new Delete(indexKey);
    htable.delete(delete);
  }

  private void validateIndexEntry(IndexEntry indexEntry) {
    for (Map.Entry<String, Object> entry : indexEntry.getFields().entrySet()) {
      IndexFieldDefinition fieldDef = definition.getField(entry.getKey());
      if (fieldDef == null) {
        throw new MalformedIndexEntryException("Index entry contains a field that is not part of " +
          "the index definition: " + entry.getKey());
      }

      if (entry.getValue() != null) {
        if (!fieldDef.getType().supportsType(entry.getValue().getClass())) {
          throw new MalformedIndexEntryException("Index entry for field " + entry.getKey() + " contains" +
            " a value of an incorrect type. Expected: " + fieldDef.getType().getType().getName() +
            ", found: " + entry.getValue().getClass().getName());
        }
      }
    }
  }

  /**
   * Build the index row key.
   *
   * <p>The format is as follows:
   *
   * <pre>
   * ([1 byte field flags][value as bytes][end-of-field marker in case of variable-length fields)*[identifier]
   * </pre>
   *
   * <p>The field flags are currently used to mark if a field is null
   * or not. If a field is null, its value will be encoded as all-zero bits.
   */
  private byte[] buildRowKey(IndexEntry entry, byte[] identifier) {
    List<byte[]> keyComponents = new ArrayList<byte[]>();

    for (IndexFieldDefinition fieldDef : definition.getFields()) {
      Object value = entry.getValue(fieldDef.getName());
      byte[] bytes = fieldToBytes(fieldDef, value, true);
      keyComponents.add(bytes);
    }

    byte[] encodedIdentifier = IdentifierEncoding.encode(identifier);

    // Calculate length of the index key
    int keyLength = 0;
    for (byte[] bytes : keyComponents) {
      keyLength += bytes.length;
    }
    keyLength += encodedIdentifier.length;

    // Create the index key
    // Add all the fields
    byte[] indexKey = new byte[keyLength];
    int pos = 0;
    for (byte[] bytes : keyComponents) {
      System.arraycopy(bytes, 0, indexKey, pos, bytes.length);
      pos += bytes.length;
    }

    // Add the identifier
    System.arraycopy(encodedIdentifier, 0, indexKey, pos, encodedIdentifier.length);

    if (definition.getIdentifierOrder() == Order.DESCENDING) {
      invertBits(indexKey, pos, indexKey.length);
    }

    return indexKey;
  }

  /**
   *
   * @param includeEndMarker for variable-length fields, indicates that the end-of-field marker should be included
   */
  private byte[] fieldToBytes(IndexFieldDefinition fieldDef, Object value, boolean includeEndMarker) {
    byte[] valueAsBytes;
    if (value != null) {
      valueAsBytes = fieldDef.toBytes(value);
    } else {
      valueAsBytes = new byte[0];
    }

    byte[] eof = includeEndMarker ? fieldDef.getEndOfFieldMarker() : EMPTY_BYTE_ARRAY;

    //
    // Construct the result, which consists of flags + value + eof
    //
    int totalLength = FIELD_FLAGS_SIZE + valueAsBytes.length + eof.length;

    byte[] bytes = new byte[totalLength];
    if (value == null) {
      bytes[0] = setNullFlag((byte)0);
    }

    System.arraycopy(valueAsBytes, 0, bytes, 1, valueAsBytes.length);

    System.arraycopy(eof, 0, bytes, valueAsBytes.length + 1, eof.length);

    if (fieldDef.getOrder() == Order.DESCENDING) {
      // we invert everything, including the field flags (which is not really necessary)
      invertBits(bytes, 0, bytes.length);
    }

    return bytes;
  }

  private void invertBits(byte[] bytes, int startOffset, int endOffset) {
    for (int i = startOffset; i < endOffset; i++) {
      bytes[i] ^= 0xFF;
    }
  }

  private byte setNullFlag(byte flags) {
    return (byte)(flags | 0x01);
  }

  public QueryResult performQuery(Query query) throws IOException {
    // First validate that all the fields used in the query exist in the index definition
    for (Query.EqualsCondition eqCond : query.getEqConditions()) {
      if (definition.getField(eqCond.getName()) == null) {
        String msg = String.format("The query refers to a field which does not exist in this index: %1$s",
          eqCond.getName());
        throw new MalformedQueryException(msg);
      }
    }
    if (query.getRangeCondition() != null && definition.getField(query.getRangeCondition().getName()) == null) {
      String msg = String.format("The query refers to a field which does not exist in this index: %1$s",
        query.getRangeCondition().getName());
      throw new MalformedQueryException(msg);
    }

    // Construct from and to keys

    List<IndexFieldDefinition> fieldDefs = definition.getFields();

    List<byte[]> fromKeyComponents = new ArrayList<byte[]>(fieldDefs.size());
    byte[] fromKey = null;
    byte[] toKey = null;

    Query.RangeCondition rangeCond = query.getRangeCondition();
    boolean rangeCondSet = false;
    int usedConditionsCount = 0;
    int i = 0;
    for (; i < fieldDefs.size(); i++) {
      IndexFieldDefinition fieldDef = fieldDefs.get(i);

      Query.EqualsCondition eqCond = query.getCondition(fieldDef.getName());
      if (eqCond != null) {
        checkQueryValueType(fieldDef, eqCond.getValue());
        byte[] bytes = fieldToBytes(fieldDef, eqCond.getValue(), true);
        fromKeyComponents.add(bytes);
        usedConditionsCount++;
      } else if (rangeCond != null) {
        if (!rangeCond.getName().equals(fieldDef.getName())) {
          throw new MalformedQueryException("Query defines range condition on field " + rangeCond.getName() +
            " but has no equals condition on field " + fieldDef.getName() +
            " which comes earlier in the index definition.");
        }

        List<byte[]> toKeyComponents = new ArrayList<byte[]>(fromKeyComponents.size() + 1);
        toKeyComponents.addAll(fromKeyComponents);

        Object fromValue = query.getRangeCondition().getFromValue();
        Object toValue = query.getRangeCondition().getToValue();

        if (fromValue == Query.MIN_VALUE) {
          // just leave of the value, a shorter key is smaller than anything else
        } else {
          checkQueryValueType(fieldDef, fromValue);
          byte[] bytes = fieldToBytes(fieldDef, fromValue, false);
          fromKeyComponents.add(bytes);
        }

        if (toValue == Query.MAX_VALUE) {
          // Searching to max value is equal to a prefix search (assumes always exclusive interval,
          // since max value is bigger than anything else)
          // So, append nothing to the search key.
        } else {
          checkQueryValueType(fieldDef, toValue);
          byte[] bytes = fieldToBytes(fieldDef, toValue, false);
          toKeyComponents.add(bytes);
        }

        fromKey = concat(fromKeyComponents);
        toKey = concat(toKeyComponents);

        rangeCondSet = true;
        usedConditionsCount++;

        break;
      } else {
        // we're done
        break;
      }
    }

    // Check if we have used all conditions defined in the query
    if (i < fieldDefs.size() && usedConditionsCount < query.getEqConditions().size() + (rangeCond != null ? 1 : 0)) {
      StringBuilder message = new StringBuilder();
      message.append("The query contains conditions on fields which either did not follow immediately on ");
      message.append("the previous equals condition or followed after a range condition on a field. The fields are: ");
      for (; i < fieldDefs.size(); i++) {
        IndexFieldDefinition fieldDef = fieldDefs.get(i);
        if (query.getCondition(fieldDef.getName()) != null) {
          message.append(fieldDef.getName());
        } else if (rangeCond != null && rangeCond.getName().equals(fieldDef.getName())) {
          message.append(fieldDef.getName());
        }
        message.append(" ");
      }
      throw new MalformedQueryException(message.toString());
    }

    if (!rangeCondSet) {
      // Construct fromKey/toKey for the case there were only equals conditions
      fromKey = concat(fromKeyComponents);
      toKey = fromKey;
    }

    Scan scan = new Scan(fromKey);

    // Query.MAX_VALUE is a value which should be larger than anything, so cannot be an inclusive upper bound
    // The importance of this is because for Query.MAX_VALUE, we do a prefix scan so the operator should be
    // CompareOp.LESS_OR_EQUAL
    boolean upperBoundInclusive = rangeCond != null && (rangeCond.isUpperBoundInclusive() || rangeCond.getToValue() == Query.MAX_VALUE);
    CompareOp op = rangeCondSet && !upperBoundInclusive ? CompareOp.LESS : CompareOp.LESS_OR_EQUAL;
    Filter toFilter = new RowFilter(op, new BinaryPrefixComparator(toKey));

    if (rangeCondSet && !rangeCond.isLowerBoundInclusive()) {
      // TODO: optimize the performance hit caused by the extra filter
      //  Once the greater filter on the fromKey returns true, it will remain true because
      //  row keys are sorted. The RowFilter will however keep doing the check again and again
      //  on each new row key. We need a new filter in HBase, something like the opposite of the
      //  WhileMatchFilter.
      FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filters.addFilter(new RowFilter(CompareOp.GREATER, new BinaryPrefixComparator(fromKey)));
      filters.addFilter(toFilter);
      scan.setFilter(filters);
    } else {
      scan.setFilter(toFilter);
    }

    return new ScannerQueryResult(htable.getScanner(scan), definition.getIdentifierOrder() == Order.DESCENDING);
  }

  private void checkQueryValueType(IndexFieldDefinition fieldDef, Object value) {
    if (value != null && !fieldDef.getType().supportsType(value.getClass())) {
      throw new MalformedQueryException("Query includes a condition on field " + fieldDef.getName() + " with" +
        " a value of an incorrect type. Expected: " + fieldDef.getType().getType().getName() +
        ", found: " + value.getClass().getName());
    }
  }

  private byte[] concat(List<byte[]> list) {
    int length = 0;
    for (byte[] bytes : list) {
      length += bytes.length;
    }
    byte[] result = new byte[length];
    int pos = 0;
    for (byte[] bytes : list) {
      System.arraycopy(bytes, 0, result, pos, bytes.length);
      pos += bytes.length;
    }
    return result;
  }

}
