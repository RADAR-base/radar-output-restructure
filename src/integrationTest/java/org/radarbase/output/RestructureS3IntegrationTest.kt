package org.radarbase.output

import io.minio.PutObjectOptions
import io.minio.PutObjectOptions.MAX_PART_SIZE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.config.S3Config
import org.radarbase.output.util.Timer
import java.nio.file.Paths

class RestructureS3IntegrationTest {
    @Test
    fun integration() {
        Timer.isEnabled = true
        val sourceConfig = S3Config(
                endpoint ="http://localhost:9000",
                accessToken = "minioadmin",
                secretKey = "minioadmin",
                bucket = "source")
        val targetConfig = S3Config(
                endpoint ="http://localhost:9000",
                accessToken = "minioadmin",
                secretKey = "minioadmin",
                bucket = "target")
        val config = RestructureConfig(
                source = ResourceConfig("s3", s3 = sourceConfig),
                target = ResourceConfig("s3", s3 = targetConfig),
                paths = PathConfig(inputs = listOf(Paths.get("in")))
        )
        val application = Application(config)
        val sourceClient = sourceConfig.createS3Client()
        if (!sourceClient.bucketExists(sourceConfig.bucket)) {
            sourceClient.makeBucket(sourceConfig.bucket)
        }

        val statusFileName = Paths.get("in/application_server_status/partition=1/application_server_status+1+0000000018+0000000020.avro")
        javaClass.getResourceAsStream("/application_server_status/application_server_status+1+0000000018+0000000020.avro").use { statusFile ->
            sourceClient.putObject(sourceConfig.bucket, statusFileName.toString(), statusFile, PutObjectOptions(-1, MAX_PART_SIZE))
        }

        application.start()

        val targetClient = targetConfig.createS3Client()
        val files = targetClient.listObjects(targetConfig.bucket, "output")
                .map { it.get().objectName() }
                .toList()

        application.redisPool.resource.use { redis ->
            assertEquals(1L, redis.del("offsets/application_server_status.json"))
        }

        val outputFolder = "output/STAGING_PROJECT/1543bc93-3c17-4381-89a5-c5d6272b827c/application_server_status"
        assertEquals(
                listOf(
                        "$outputFolder/20200128_1300.csv",
                        "$outputFolder/20200128_1400.csv",
                        "$outputFolder/schema-application_server_status.json"),
                files)

        sourceClient.removeObject(sourceConfig.bucket, statusFileName.toString())
        sourceClient.removeBucket(sourceConfig.bucket)
        files.forEach {
            targetClient.removeObject(targetConfig.bucket, it)
        }
        targetClient.removeBucket(targetConfig.bucket)

        println(Timer)
    }
}
