package org.chen.service.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

public interface HBaseClient {

  void stop(String string);

  void delete(String table, Delete delete) throws IOException;

  void putAsync(String table, Put put) throws IOException;

  void put(String table, Put put) throws IOException;

  void flushCommits(String table) throws IOException;

}
