package org.radarbase.output.source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.radarbase.output.config.HdfsConfig
import org.radarbase.output.config.ResourceConfig
import java.nio.file.Path

class HdfsSourceStorageFactory(
    private val resourceConfig: ResourceConfig,
    private val tempPath: Path,
) {
    fun createSourceStorage(): SourceStorage {
        val hdfsConfig = requireNotNull(resourceConfig.hdfs)
        val fileSystem = FileSystem.get(hdfsConfig.configuration())
        return HdfsSourceStorage(fileSystem)
    }

    companion object {
        fun HdfsConfig.configuration(): Configuration {
            val configuration = Configuration()

            configuration["fs.hdfs.impl.disable.cache"] = "true"
            if (nameNodes.size == 1) {
                configuration["fs.defaultFS"] = "hdfs://${nameNodes.first()}:8020"
            }
            if (nameNodes.size >= 2) {
                val clusterId = "radarCluster"
                configuration["fs.defaultFS"] = "hdfs://$clusterId"
                configuration["dfs.nameservices"] = clusterId
                configuration["dfs.ha.namenodes.$clusterId"] = nameNodes.indices.joinToString(",") { "nn$it" }

                nameNodes.forEachIndexed { i, hostname ->
                    configuration["dfs.namenode.rpc-address.$clusterId.nn$i"] = "$hostname:8020"
                }

                configuration["dfs.client.failover.proxy.provider.$clusterId"] = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            }

            return configuration
        }
    }
}
