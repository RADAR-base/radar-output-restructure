import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.avast.gradle.dockercompose.ComposeSettings

plugins {
    kotlin("jvm")
    application
    `maven-publish`
    signing
    id("org.jetbrains.dokka") version "1.4.30"
    id("com.avast.gradle.docker-compose") version "0.14.2"
    id("com.github.ben-manes.versions") version "0.38.0"
}

group = "org.radarbase"
version = "1.1.7-SNAPSHOT"

description = "RADAR-base output restructuring"
val website = "https://radar-base.org"
val githubRepoName = "RADAR-base/Restructure-HDFS-topic"
val githubUrl = "https://github.com/${githubRepoName}"
val issueUrl = "${githubUrl}/issues"

application {
    mainClass.set("org.radarbase.output.Application")
}

repositories {
    mavenCentral()
}

sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

configurations["integrationTestImplementation"].extendsFrom(
    configurations.implementation.get(),
    configurations.testImplementation.get()
)
configurations["integrationTestRuntimeOnly"].extendsFrom(
    configurations.runtimeOnly.get(),
    configurations.testRuntimeOnly.get()
)

dependencies {
    val avroVersion: String by project
    api("org.apache.avro:avro:$avroVersion")

    val jacksonVersion: String by project
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    val jedisVersion: String by project
    implementation("redis.clients:jedis:$jedisVersion")

    val jCommanderVersion: String by project
    implementation("com.beust:jcommander:$jCommanderVersion")

    val almworksVersion: String by project
    implementation("com.almworks.integers:integers:$almworksVersion")

    val minioVersion: String by project
    implementation("io.minio:minio:$minioVersion")

    val azureStorageVersion: String by project
    implementation("com.azure:azure-storage-blob:$azureStorageVersion")
    implementation("com.opencsv:opencsv:5.4")

    implementation("org.apache.avro:avro-mapred:$avroVersion")

    val hadoopVersion: String by project
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion")

    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    runtimeOnly("org.slf4j:slf4j-log4j12:$slf4jVersion")
    runtimeOnly("org.apache.hadoop:hadoop-hdfs-client:$hadoopVersion")

    val radarSchemasVersion: String by project
    testImplementation("org.radarcns:radar-schemas-commons:$radarSchemasVersion")

    val junitVersion: String by project
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")

    val kotlinVersion: String by project
    dokkaHtmlPlugin("org.jetbrains.dokka:kotlin-as-java-plugin:$kotlinVersion")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
        apiVersion = "1.4"
        languageVersion = "1.4"
    }
}

distributions {
    main {
        contents {
            into("share/${project.name}") {
                from("README.md", "LICENSE")
            }
        }
    }
}

tasks.startScripts {
    classpath = files("lib/PlaceHolderForPluginPath")

    doLast {
        val windowsScriptFile = file(getWindowsScript())
        val unixScriptFile    = file(getUnixScript())
        windowsScriptFile.writeText(windowsScriptFile.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins\\*"))
        unixScriptFile.writeText(unixScriptFile.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins/*"))
    }
}

val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    shouldRunAfter("test")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
        setExceptionFormat(FULL)
    }
}

val check by tasks
check.dependsOn(integrationTest)
project.dockerCompose.isRequiredBy(integrationTest)

tasks.withType<Tar> {
    compression = Compression.GZIP
    archiveExtension.set("tar.gz")
}

tasks.register("downloadDependencies") {
    doLast {
        description = "Pre-downloads dependencies"
        configurations.compileClasspath.get().files
        configurations.runtimeClasspath.get().files
    }
    outputs.upToDateWhen { false }
}

tasks.register<Copy>("copyDependencies") {
    from(configurations.runtimeClasspath.get().files)
    into("$buildDir/third-party/")
}

// custom tasks for creating source/javadoc jars
val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
    dependsOn(tasks.classes)
}

val dokkaJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    from("$buildDir/dokka/javadoc/")
    dependsOn(tasks.dokkaJavadoc)
}

tasks.withType<DokkaTask> {
    logging.level = LogLevel.QUIET
}

tasks.withType<Jar> {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJar") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(dokkaJar)
            pom {
                name.set(project.name)
                url.set(githubUrl)
                description.set(project.description)

                licenses {
                    license {
                        name.set("The Apache Software License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        id.set("blootsvoets")
                        name.set("Joris Borgdorff")
                        email.set("joris@thehyve.nl")
                        organization.set("The Hyve")
                    }

                }
                issueManagement {
                    system.set("GitHub")
                    url.set(issueUrl)
                }
                organization {
                    name.set("RADAR-base")
                    url.set(website)
                }
                scm {
                    connection.set("scm:git:${githubUrl}")
                    url.set(githubUrl)
                }
            }
        }
    }

    repositories {
        fun Project.propertyOrEnv(propertyName: String, envName: String): String? {
            return if (hasProperty(propertyName)) {
                property(propertyName)?.toString()
            } else {
                System.getenv(envName)
            }
        }

        maven {
            name = "OSSRH"
            credentials {
                username = propertyOrEnv("ossrh.user", "OSSRH_USER")
                password = propertyOrEnv("ossrh.password", "OSSRH_PASSWORD")
            }

            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

signing {
    useGpgCmd()
    isRequired = true
    sign(tasks["sourcesJar"], tasks["dokkaJar"])
    sign(publishing.publications["mavenJar"])
}

tasks.withType<Sign> {
    onlyIf { gradle.taskGraph.hasTask("${project.path}:publish") }
}

tasks.wrapper {
    gradleVersion = "6.8.3"
}
