package org.radarbase.output.config

enum class ResourceType {
    S3, HDFS, LOCAL, AZURE;

    companion object {
        fun String.toResourceType() = when (lowercase()) {
            "s3" -> S3
            "hdfs" -> HDFS
            "local" -> LOCAL
            "azure" -> AZURE
            else -> null
        }
    }
}
