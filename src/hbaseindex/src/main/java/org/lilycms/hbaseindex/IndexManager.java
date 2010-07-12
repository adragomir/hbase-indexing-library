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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Starting point for all the index and query functionality.
 *
 * <p>This class should be instantiated yourself. This class is threadsafe,
 * but on the other hand rather lightweight so it does not harm to have multiple
 * instances.
 */
public class IndexManager {
  private Configuration hbaseConf;
  private HBaseAdmin hbaseAdmin;
  private String metaTableName;
  private String dataTableName;
  private HTable metaTable;
  private HTable dataTable;

  public static final String DEFAULT_META_TABLE = "indexmeta";
  public static final String DEFAULT_DATA_TABLE = "indexdata";

  /**
   * Constructor.
   *
   * <p>Calls {@link #IndexManager(Configuration, String, String) IndexManager(hbaseConf, DEFAULT_META_TABLE)}.
   */
  public IndexManager(Configuration hbaseConf) throws IOException {
    this(hbaseConf, DEFAULT_META_TABLE, DEFAULT_DATA_TABLE);
  }

  /**
   * Constructor.
   *
   * <p>The supplied metaTableName should be an existing table. You can use the utility
   * method {@link #createIndexMetaTable} to create this table.
   *
   * @param metaTableName name of the HBase table in which to manage the configuration of the indexes
   */
  public IndexManager(Configuration hbaseConf, String metaTableName, String dataTableName) throws IOException {
    this.hbaseConf = hbaseConf;
    hbaseAdmin = new HBaseAdmin(hbaseConf);
    this.metaTableName = metaTableName;
    this.dataTableName = dataTableName;
    metaTable = new HTable(hbaseConf, this.metaTableName);
    dataTable = new HTable(hbaseConf, this.dataTableName);
  }

  /**
   * Creates a new index.
   *
   * <p>This first creates the HBase table for this index, then adds the index
   * definition to the indexmeta table.
   */
  public synchronized void createIndex(IndexDefinition indexDef) throws IOException {
    if (indexDef.getFields().size() == 0) {
      throw new IllegalArgumentException("An IndexDefinition should contain at least one field.");
    }

    byte[] jsonData = serialize(indexDef);

    try {
      IndexManager.createIndexDataTable(hbaseConf, this.dataTableName);
    } catch (TableExistsException x) {
      // do nothing
    }
    
    Put put = new Put(Bytes.toBytes(indexDef.getFullName()));
    put.add(Bytes.toBytes("meta"), Bytes.toBytes("conf"), jsonData);
    metaTable.put(put);
  }

  private byte[] serialize(IndexDefinition indexDef) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(os, indexDef.toJson());
    return os.toByteArray();
  }

  private IndexDefinition deserialize(String table, String name, byte[] jsonData) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return new IndexDefinition(table, name, mapper.readValue(jsonData, 0, jsonData.length, ObjectNode.class));
  }

  /**
   * Retrieves an Index.
   *
   * @throws IndexNotFoundException if the index does not exist
   */
  public Index getIndex(String table, String name) throws IOException, IndexNotFoundException {
    Get get = new Get(Bytes.toBytes(IndexDefinition.buildIndexName(table, name)));
    Result result = metaTable.get(get);

    if (result.isEmpty())
      throw new IndexNotFoundException(table, name);

    byte[] jsonData = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("conf"));
    IndexDefinition indexDef = deserialize(table, name, jsonData);

    HTable htable = new HTable(hbaseConf, name);
    Index index = new Index(htable, indexDef);
    return index;
  }

  /**
   * Deletes an index.
   *
   * <p>This removes the index definition from the index meta table, disables the
   * index table and deletes it. If this would fail in between any of these operations,
   * it is up to the administrator to perform the remaining work.
   *
   * @throws IndexNotFoundException if the index does not exist.
   */
  public synchronized void deleteIndex(String table, String name) throws IOException, IndexNotFoundException {
    Get get = new Get(Bytes.toBytes(IndexDefinition.buildIndexName(table, name)));
    Result result = metaTable.get(get);

    if (result.isEmpty())
      throw new IndexNotFoundException(table, name);

    // TODO what if this fails in between operations? Log this...

    Delete del = new Delete(Bytes.toBytes(IndexDefinition.buildIndexName(table, name)));
    metaTable.delete(del);

    //TODO: background mr to delete the indexed rows
  }

  /**
   * Utility method for creating the indexmeta table.
   */
  public static void createIndexMetaTable(Configuration hbaseConf, String metaTableName) throws IOException {
    HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
    HTableDescriptor table = new HTableDescriptor(metaTableName);
    HColumnDescriptor family = new HColumnDescriptor("meta");
    table.addFamily(family);
    hbaseAdmin.createTable(table);
  }

  public static void createIndexDataTable(Configuration hbaseConf, String dataTableName) throws IOException {
    HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
    HTableDescriptor table = new HTableDescriptor(dataTableName);
    HColumnDescriptor family = new HColumnDescriptor(Index.DATA_FAMILY);
    table.addFamily(family);
    hbaseAdmin.createTable(table);
  }

  /**
   * Utility method for creating the indexmeta table.
   */
  public static void createIndexMetaTable(Configuration hbaseConf) throws IOException {
    IndexManager.createIndexMetaTable(hbaseConf, IndexManager.DEFAULT_META_TABLE);
  }

  public static void createIndexDataTable(Configuration hbaseConf) throws IOException {
    IndexManager.createIndexDataTable(hbaseConf, IndexManager.DEFAULT_DATA_TABLE);
  }

}
