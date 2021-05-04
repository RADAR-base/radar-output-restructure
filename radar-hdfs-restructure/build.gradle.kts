import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.time.Duration

plugins {
    kotlin("jvm")
    application
    id("com.github.ben-manes.versions")
}

description = "RADAR-base HDFS restructuring"

dependencies {
    api(project(":"))

    val avroVersion: String by project
    implementation("org.apache.avro:avro-mapred:$avroVersion")

    val hadoopVersion: String by project
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion")

    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    runtimeOnly("org.slf4j:slf4j-log4j12:$slf4jVersion")
    runtimeOnly("org.apache.hadoop:hadoop-hdfs-client:$hadoopVersion")
}
