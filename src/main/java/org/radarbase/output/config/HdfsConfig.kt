package org.radarbase.output.config

data class HdfsConfig(
    /** HDFS name nodes to use. */
    val nameNodes: List<String> = emptyList(),
    /** Additional HDFS configuration parameters. */
    val properties: Map<String, String> = emptyMap(),
) {
    fun validate() {
        check(nameNodes.isNotEmpty()) { "Cannot use HDFS without any name nodes." }
    }
}
