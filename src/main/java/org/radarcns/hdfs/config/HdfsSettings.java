package org.radarcns.hdfs.config;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HdfsSettings {
    private static final Logger logger = LoggerFactory.getLogger(HdfsSettings.class);

    private final Configuration configuration;
    private String hdfsName;

    private HdfsSettings(Builder builder) {
        this.hdfsName = builder.hdfsName;
        this.configuration = new Configuration();
        builder.hdfsConf.forEach(this.configuration::set);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getHdfsName() {
        return hdfsName;
    }

    public static class Builder {
        private String hdfsName;
        private final Map<String, String> hdfsConf = new HashMap<>();

        public Builder(final String name) {
            this.hdfsName = name;
        }

        public Builder hdfsHighAvailability(String hdfsHa, String hdfsNameNode1, String hdfsNameNode2) {
            // Configure high availability
            if (hdfsHa == null && hdfsNameNode1 == null && hdfsNameNode2 == null) {
                return this;
            }
            if (hdfsHa == null || hdfsNameNode1 == null || hdfsNameNode2 == null) {
                throw new IllegalArgumentException("HDFS High availability name node configuration is incomplete.");
            }

            String[] haNames = hdfsHa.split(",");
            if (haNames.length != 2) {
                logger.error("Cannot process HDFS High Availability mode for other than two name nodes.");
                System.exit(1);
            }

            putHdfsConfig("dfs.nameservices", hdfsName)
                    .putHdfsConfig("dfs.ha.namenodes." + hdfsName, hdfsHa)
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsName + "." + haNames[0],
                            hdfsNameNode1 + ":8020")
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsName + "." + haNames[1],
                            hdfsNameNode2 + ":8020")
                    .putHdfsConfig("dfs.client.failover.proxy.provider." + hdfsName,
                            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

            return this;
        }

        public Builder putHdfsConfig(String name, String value) {
            hdfsConf.put(name, value);
            return this;
        }

        public HdfsSettings build() {
            return new HdfsSettings(this);
        }
    }
}
