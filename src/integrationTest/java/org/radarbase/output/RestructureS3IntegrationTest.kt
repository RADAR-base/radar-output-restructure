package org.radarbase.output

import io.minio.BucketExistsArgs
import io.minio.GetObjectArgs
import io.minio.ListObjectsArgs
import io.minio.MakeBucketArgs
import io.minio.ObjectWriteArgs.MAX_PART_SIZE
import io.minio.PutObjectArgs
import io.minio.RemoveBucketArgs
import io.minio.RemoveObjectArgs
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.output.config.PathConfig
import org.radarbase.output.config.PathFormatterConfig
import org.radarbase.output.config.ResourceConfig
import org.radarbase.output.config.RestructureConfig
import org.radarbase.output.config.S3Config
import org.radarbase.output.config.TopicConfig
import org.radarbase.output.config.WorkerConfig
import org.radarbase.output.util.SuspendedCloseable.Companion.useSuspended
import org.radarbase.output.util.Timer
import org.radarbase.output.util.bucketBuild
import org.radarbase.output.util.objectBuild
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths

class RestructureS3IntegrationTest {
    @Test
    fun integration() = runTest {
        Timer.isEnabled = true
        val sourceConfig = S3Config(
            endpoint = "http://localhost:9000",
            accessToken = "minioadmin",
            secretKey = "minioadmin",
            bucket = "source",
        )
        val targetConfig = sourceConfig.copy(bucket = "target")
        val topicConfig = mapOf(
            "application_server_status" to TopicConfig(
                pathProperties = PathFormatterConfig(
                    format = "\${projectId}/\${userId}/\${topic}/\${value:serverStatus}/\${filename}",
                ),
            ),
        )
        val config = RestructureConfig(
            source = ResourceConfig("s3", s3 = sourceConfig),
            target = ResourceConfig("s3", s3 = targetConfig),
            paths = PathConfig(inputs = listOf(Paths.get("in"))),
            worker = WorkerConfig(minimumFileAge = 0L),
            topics = topicConfig,
        )
        val application = Application(config)
        val sourceClient = sourceConfig.createS3Client()
        val sourceBucket = requireNotNull(sourceConfig.bucket)
        if (!sourceClient.bucketExists(BucketExistsArgs.builder().bucketBuild(sourceBucket))) {
            sourceClient.makeBucket(MakeBucketArgs.builder().bucketBuild(sourceBucket))
        }

        val resourceFiles = listOf(
            "application_server_status/partition=1/application_server_status+1+0000000018+0000000020.avro",
            "application_server_status/partition=1/application_server_status+1+0000000021.avro",
            "android_phone_acceleration/partition=0/android_phone_acceleration+0+0003018784.avro",
        )
        val targetFiles = resourceFiles.map { Paths.get("in/$it") }
        resourceFiles.mapIndexed { i, resourceFile ->
            launch(Dispatchers.IO) {
                this@RestructureS3IntegrationTest.javaClass.getResourceAsStream("/$resourceFile")
                    .useSuspended { statusFile ->
                        sourceClient.putObject(
                            PutObjectArgs.Builder()
                                .objectBuild(sourceBucket, targetFiles[i]) {
                                    stream(statusFile, -1, MAX_PART_SIZE)
                                },
                        )
                    }
            }
        }.joinAll()

        application.start()

        val targetClient = targetConfig.createS3Client()

        application.redisHolder.execute { redis ->
            launch { assertEquals(1L, redis.del("offsets/application_server_status.json")) }
            launch { assertEquals(1L, redis.del("offsets/android_phone_acceleration.json")) }
        }

        val firstParticipantOutput =
            "output/STAGING_PROJECT/1543bc93-3c17-4381-89a5-c5d6272b827c/application_server_status/CONNECTED"
        val secondParticipantOutput =
            "output/radar-test-root/4ab9b985-6eec-4e51-9a29-f4c571c89f99/android_phone_acceleration"

        val targetBucket = requireNotNull(targetConfig.bucket)

        val files = coroutineScope {
            launch(Dispatchers.IO) {
                val csvContents = """
                key.projectId,key.userId,key.sourceId,value.time,value.serverStatus,value.ipAddress
                STAGING_PROJECT,1543bc93-3c17-4381-89a5-c5d6272b827c,99caf236-bbe6-4eed-9c63-fba77349821d,1.58021982003E9,CONNECTED,
                STAGING_PROJECT,1543bc93-3c17-4381-89a5-c5d6272b827c,99caf236-bbe6-4eed-9c63-fba77349821d,1.58021982003E9,CONNECTED,

                """.trimIndent()

                val targetContent = targetClient.getObject(
                    GetObjectArgs.Builder().bucketBuild(targetBucket) {
                        `object`("$firstParticipantOutput/20200128_1300.csv")
                    },
                ).use { response ->
                    response.readBytes()
                }

                assertEquals(csvContents, targetContent.toString(UTF_8))
            }

            return@coroutineScope withContext(Dispatchers.IO) {
                targetClient.listObjects(
                    ListObjectsArgs.Builder().bucketBuild(targetBucket) {
                        prefix("output")
                        recursive(true)
                        useUrlEncodingType(false)
                    },
                )
                    .mapTo(HashSet()) { it.get().objectName() }
            }
        }

        assertEquals(
            hashSetOf(
                "$firstParticipantOutput/20200128_1300.csv",
                "$firstParticipantOutput/20200128_1400.csv",
                "$firstParticipantOutput/schema-application_server_status.json",
                "$secondParticipantOutput/20200528_1000.csv",
                "$secondParticipantOutput/schema-android_phone_acceleration.json",
            ),
            files,
        )

        coroutineScope {
            // delete source files
            launch {
                targetFiles.map {
                    launch(Dispatchers.IO) {
                        sourceClient.removeObject(
                            RemoveObjectArgs.Builder().objectBuild(sourceBucket, it),
                        )
                    }
                }.joinAll()

                launch(Dispatchers.IO) {
                    sourceClient.removeBucket(
                        RemoveBucketArgs.Builder().bucketBuild(sourceBucket),
                    )
                }
            }

            // delete target files
            launch {
                files.map {
                    launch(Dispatchers.IO) {
                        targetClient.removeObject(
                            RemoveObjectArgs.Builder().bucketBuild(targetBucket) {
                                `object`(it)
                            },
                        )
                    }
                }.joinAll()
                launch(Dispatchers.IO) {
                    targetClient.removeBucket(
                        RemoveBucketArgs.Builder().bucketBuild(targetBucket),
                    )
                }
            }
        }
        println(Timer)
    }
}
