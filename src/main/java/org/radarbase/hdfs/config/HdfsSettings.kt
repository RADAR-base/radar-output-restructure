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

package org.radarbase.hdfs.config

import java.util.HashMap
import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class HdfsSettings private constructor(builder: Builder) {
    val configuration = Configuration()
    val hdfsName: String = builder.hdfsName

    init {
        builder.hdfsConf.forEach { (name, value) -> this.configuration.set(name, value) }
    }

    class Builder(val hdfsName: String) {
        internal val hdfsConf = HashMap<String, String>()

        init {
            this.hdfsConf["fs.defaultFS"] = "hdfs://$hdfsName"
            this.hdfsConf["fs.hdfs.impl.disable.cache"] = "true"
        }

        fun hdfsHighAvailability(hdfsHa: String?, hdfsNameNode1: String?, hdfsNameNode2: String?): Builder {
            // Configure high availability
            if (hdfsHa == null && hdfsNameNode1 == null && hdfsNameNode2 == null) {
                return this
            }
            require(hdfsHa != null && hdfsNameNode1 != null && hdfsNameNode2 != null) { "HDFS High availability name node configuration is incomplete." }

            val haNames = hdfsHa.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            if (haNames.size != 2) {
                logger.error("Cannot process HDFS High Availability mode for other than two name nodes.")
                exitProcess(1)
            }

            putHdfsConfig("dfs.nameservices", hdfsName)
                    .putHdfsConfig("dfs.ha.namenodes.$hdfsName", hdfsHa)
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsName + "." + haNames[0],
                            "$hdfsNameNode1:8020")
                    .putHdfsConfig("dfs.namenode.rpc-address." + hdfsName + "." + haNames[1],
                            "$hdfsNameNode2:8020")
                    .putHdfsConfig("dfs.client.failover.proxy.provider.$hdfsName",
                            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

            return this
        }

        fun putHdfsConfig(name: String, value: String): Builder {
            hdfsConf[name] = value
            return this
        }

        fun build(): HdfsSettings = HdfsSettings(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(HdfsSettings::class.java)
    }
}
