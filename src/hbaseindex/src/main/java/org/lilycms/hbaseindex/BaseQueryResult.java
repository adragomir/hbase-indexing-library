package org.lilycms.hbaseindex;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

abstract class BaseQueryResult implements QueryResult {
    protected Result currentResult;
    protected QueryResult currentQResult;

    public byte[] getData(byte[] qualifier) {
        if (currentResult != null) {
            return currentResult.getValue(Index.DATA_FAMILY, qualifier);
        } else if (currentQResult != null) {
            return currentQResult.getData(qualifier);
        } else {
            throw new RuntimeException("QueryResult.getData() is being called but there is no current result.");
        }
    }

    public byte[] getData(String qualifier) {
        return getData(Bytes.toBytes(qualifier));
    }

    public String getDataAsString(String qualifier) {
        return Bytes.toString(getData(Bytes.toBytes(qualifier)));
    }
}
