package org.chen.service.hbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

public class HBaseSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkTask.class);
  private String table;
  private String zkServers;
  private String username;
  private String password;
  private boolean isAsync = true;
  private HBaseClient client;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    table = props.get(HBaseSinkConnector.TABLE_CONFIG);
    zkServers = props.get(HBaseSinkConnector.ZK_CONFIG);
    username = props.get(HBaseSinkConnector.USER_CONFIG);
    password = props.get(HBaseSinkConnector.PWD_CONFIG);
    isAsync = "async".equalsIgnoreCase(props.get(HBaseSinkConnector.MODLE_CONFIG));
    // init conf
    Configuration conf = new Configuration(false);
    conf.setBoolean("hbase.cluster.managed", false);
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hbase.security.authorization", "true");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@REALM.COM");
    conf.set("hbase.master.kerberos.principal", "hbase/_HOST@REALM.COM");
    conf.set("hbase.client.kerberos.principal", username + "@REALM.COM");
    conf.set("hbase.client.keytab.password", password);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkServers);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    try {
      Class<?> clazz = Class.forName(HBaseClient.class.getName());
      Constructor<?> constructor = clazz.getDeclaredConstructor();
      constructor.setAccessible(true);
      client = (HBaseClient) constructor.newInstance();
	} catch (Exception e) {
      LOG.error("Init HBaseClient failed.", e);
	}
  }

  @Override
  public void stop() {
    if (client != null) {
      client.stop("HBaseSinkTask exit.");
    }
  }

  private void putRecord(SinkRecord record, WALEdit value) throws IOException {
    Mutation mutation = null;
    if (value != null && value.getCells().size() > 0) {
      Cell kv = value.getCells().get(0);
      if (CellUtil.isDelete(kv)) {
        mutation = new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), record.timestamp());
      } else {
        mutation = new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), record.timestamp());
      }
    } else {
      return;
    }
    mutation.setDurability(Durability.SKIP_WAL);
    if (mutation instanceof Delete) {
      Delete delete = (Delete)mutation;
      for (Cell kv : value.getCells()) {
        delete.addDeleteMarker(kv);
      }
      client.delete(table, delete);
    } else {
      Put put = (Put)mutation;
      for (Cell kv : value.getCells()) {
        put.add(kv);
      }
      if (this.isAsync) {
        client.putAsync(table, put);
      } else {
        client.put(table, put);
      }
    }
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      byte[] value = (byte[]) record.value();
      ByteArrayDataInput valueIn = ByteStreams.newDataInput(value);
      try {
        WALEdit edits = new WALEdit();
        edits.readFields(valueIn);
        putRecord(record, edits);
      } catch (Exception e) {
        LOG.error("kafka record read error or hbase put error, offset info : {}-{}-{}",
            record.topic(), record.kafkaPartition(), record.kafkaOffset());
        // error data to dead letter queue
        throw new KafkaException("sink record to hbase failed.", e);
      }
    }
    LOG.debug("sink {} records to hbase.", sinkRecords.size());
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    if (this.isAsync) {
      try {
        client.flushCommits(table);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }
}
