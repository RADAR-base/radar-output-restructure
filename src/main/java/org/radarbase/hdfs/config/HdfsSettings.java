/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.hdfs.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            this.hdfsConf.put("fs.defaultFS", "hdfs://" + name);
            this.hdfsConf.put("fs.hdfs.impl.disable.cache", "true");
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
