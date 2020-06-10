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

        val resourceFiles = listOf(
                "application_server_status/partition=1/application_server_status+1+0000000018+0000000020.avro",
                "application_server_status/partition=1/application_server_status+1+0000000021.avro",
                "android_phone_acceleration/partition=0/android_phone_acceleration+0+0003018784.avro"
        )
        val targetFiles = resourceFiles.map { Paths.get("in/$it") }
        resourceFiles.forEachIndexed { i, resourceFile ->
            javaClass.getResourceAsStream("/$resourceFile").use { statusFile ->
                sourceClient.putObject(sourceConfig.bucket, targetFiles[i].toString(), statusFile, PutObjectOptions(-1, MAX_PART_SIZE))
            }
        }

        application.start()

        val targetClient = targetConfig.createS3Client()
        val files = targetClient.listObjects(targetConfig.bucket, "output")
                .map { it.get().objectName() }
                .toList()

        application.redisPool.resource.use { redis ->
            assertEquals(1L, redis.del("offsets/application_server_status.json"))
            assertEquals(1L, redis.del("offsets/android_phone_acceleration.json"))
        }

        val firstParticipantOutput = "output/STAGING_PROJECT/1543bc93-3c17-4381-89a5-c5d6272b827c/application_server_status"
        val secondParticipantOutput = "output/radar-test-root/4ab9b985-6eec-4e51-9a29-f4c571c89f99/android_phone_acceleration"
        assertEquals(
                listOf(
                        "$firstParticipantOutput/20200128_1300.csv",
                        "$firstParticipantOutput/20200128_1400.csv",
                        "$firstParticipantOutput/schema-application_server_status.json",
                        "$secondParticipantOutput/20200528_1000.csv",
                        "$secondParticipantOutput/schema-android_phone_acceleration.json"),
                files)

        targetFiles.forEach {
            sourceClient.removeObject(sourceConfig.bucket, it.toString())
        }
        sourceClient.removeBucket(sourceConfig.bucket)
        files.forEach {
            targetClient.removeObject(targetConfig.bucket, it)
        }
        targetClient.removeBucket(targetConfig.bucket)

        println(Timer)
    }
}
