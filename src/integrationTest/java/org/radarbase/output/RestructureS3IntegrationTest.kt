package org.radarbase.output

import io.minio.*
import io.minio.PutObjectOptions.MAX_PART_SIZE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.config.*
import org.radarbase.output.util.Timer
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import java.nio.charset.StandardCharsets.UTF_8
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
                paths = PathConfig(inputs = listOf(Paths.get("in"))),
                worker = WorkerConfig(minimumFileAge = 0L)
        )
        val application = Application(config)
        val sourceClient = sourceConfig.createS3Client()
        if (!sourceClient.bucketExists(BucketExistsArgs.Builder().bucketBuild(sourceConfig.bucket))) {
            sourceClient.makeBucket(MakeBucketArgs.Builder().bucketBuild(sourceConfig.bucket))
        }

        val resourceFiles = listOf(
                "application_server_status/partition=1/application_server_status+1+0000000018+0000000020.avro",
                "application_server_status/partition=1/application_server_status+1+0000000021.avro",
                "android_phone_acceleration/partition=0/android_phone_acceleration+0+0003018784.avro"
        )
        val targetFiles = resourceFiles.map { Paths.get("in/$it") }
        resourceFiles.forEachIndexed { i, resourceFile ->
            javaClass.getResourceAsStream("/$resourceFile").use { statusFile ->
                sourceClient.putObject(PutObjectArgs.Builder().objectBuild(sourceConfig.bucket, targetFiles[i]) {
                    stream(statusFile, -1, MAX_PART_SIZE)
                })
            }
        }

        application.start()

        val targetClient = targetConfig.createS3Client()
        val files = targetClient.listObjects(ListObjectsArgs.Builder().bucketBuild(targetConfig.bucket) {
                    prefix("output")
                    recursive(true)
                    useUrlEncodingType(false)
                })
                .map { it.get().objectName() }
                .toList()

        application.redisHolder.execute { redis ->
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

        println(targetClient.getObject(GetObjectArgs.builder()
                .bucketBuild(targetConfig.bucket) {
                    `object`("$firstParticipantOutput/20200128_1300.csv")
                }
        ).readBytes().toString(UTF_8))

        val csvContents = """
                key.projectId,key.userId,key.sourceId,value.time,value.serverStatus,value.ipAddress
                STAGING_PROJECT,1543bc93-3c17-4381-89a5-c5d6272b827c,99caf236-bbe6-4eed-9c63-fba77349821d,1.58021982003E9,CONNECTED,
                STAGING_PROJECT,1543bc93-3c17-4381-89a5-c5d6272b827c,99caf236-bbe6-4eed-9c63-fba77349821d,1.58021982003E9,CONNECTED,

                """.trimIndent()
        assertEquals(csvContents, targetClient.getObject(GetObjectArgs.Builder()
                .bucketBuild(targetConfig.bucket) {
                    `object`("$firstParticipantOutput/20200128_1300.csv")
                })
                .readBytes()
                .toString(UTF_8))

        targetFiles.forEach {
            sourceClient.removeObject(RemoveObjectArgs.Builder()
                    .objectBuild(sourceConfig.bucket, it))
        }
        sourceClient.removeBucket(RemoveBucketArgs.Builder().bucketBuild(sourceConfig.bucket))
        files.forEach {
            targetClient.removeObject(RemoveObjectArgs.Builder().bucketBuild(targetConfig.bucket) {
                `object`(it)
            })
        }
        targetClient.removeBucket(RemoveBucketArgs.Builder().bucketBuild(targetConfig.bucket))

        println(Timer)
    }
}
