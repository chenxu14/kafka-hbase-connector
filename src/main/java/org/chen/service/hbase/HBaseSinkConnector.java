package org.chen.service.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class HBaseSinkConnector extends SinkConnector {
  public static final String TABLE_CONFIG = "table";
  public static final String ZK_CONFIG = "zkServers";
  public static final String MODLE_CONFIG = "modle";
  public static final String USER_CONFIG = "username";
  public static final String PWD_CONFIG = "password";

  private static Validator notnull = new ConfigDef.NonNullValidator();
  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(TABLE_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, notnull, Importance.HIGH, "Destination hbase table.")
      .define(ZK_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, notnull, Importance.HIGH, "Destination hbase zk qurom.")
      .define(USER_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, notnull, Importance.HIGH, "kerberos user name.")
      .define(PWD_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, notnull, Importance.HIGH, "kerberos user password.")
      .define(MODLE_CONFIG, Type.STRING, "async", new Validator() {
        @Override
        public void ensureValid(String name, Object value) {
          if ((value == null) || (!value.equals("async") && !value.equals("sync"))) {
            throw new ConfigException("modle value should either sync or async.");
          }
        }
      }, Importance.HIGH, "client put modle (sync or async).");

  private String table;
  private String zkServers;
  private String modle;
  private String username;
  private String password;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void start(Map<String, String> props) {
    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    table = parsedConfig.getString(TABLE_CONFIG);
    zkServers = parsedConfig.getString(ZK_CONFIG);
    modle = parsedConfig.getString(MODLE_CONFIG);
    username = parsedConfig.getString(USER_CONFIG);
    password = parsedConfig.getString(PWD_CONFIG);
  }

  @Override
  public void stop() {
    // Nothing to do yet
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HBaseSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.put(TABLE_CONFIG, table);
      config.put(ZK_CONFIG, zkServers);
      config.put(MODLE_CONFIG, modle);
      config.put(USER_CONFIG, username);
      config.put(PWD_CONFIG, password);
      configs.add(config);
    }
    return configs;
  }
}
