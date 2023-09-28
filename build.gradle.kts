import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing
import java.time.Duration

plugins {
    id("application")
    id("org.radarbase.radar-root-project") version Versions.radarCommons
    id("org.radarbase.radar-dependency-management") version Versions.radarCommons
    id("org.radarbase.radar-kotlin") version Versions.radarCommons
    id("org.radarbase.radar-publishing") version Versions.radarCommons
    id("com.avast.gradle.docker-compose") version Versions.dockerCompose
}

description = "RADAR-base output restructuring"

radarRootProject {
    projectVersion.set(Versions.project)
    gradleVersion.set(Versions.wrapper)
}

radarKotlin {
    kotlinVersion.set(Versions.kotlin)
    javaVersion.set(Versions.java)
    log4j2Version.set(Versions.log4j2)
    slf4jVersion.set(Versions.slf4j)
}

radarPublishing {
    val githubRepoName = "RADAR-base/radar-output-restructure"
    githubUrl.set("https://github.com/$githubRepoName.git")
    developers {
        developer {
            id.set("blootsvoets")
            name.set("Joris Borgdorff")
            email.set("joris@thehyve.nl")
            organization.set("The Hyve")
        }
    }
}

sourceSets {
    create("integrationTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

configurations["integrationTestImplementation"].extendsFrom(
    configurations.implementation.get(),
    configurations.testImplementation.get(),
)
configurations["integrationTestRuntimeOnly"].extendsFrom(
    configurations.runtimeOnly.get(),
    configurations.testRuntimeOnly.get(),
)

dependencies {
    api("org.apache.avro:avro:${Versions.avro}")
    runtimeOnly("org.xerial.snappy:snappy-java:${Versions.snappy}")

    implementation(kotlin("reflect"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:${Versions.coroutines}")

    api(platform("com.fasterxml.jackson:jackson-bom:${Versions.jackson}"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml") {
        runtimeOnly("org.yaml:snakeyaml:${Versions.snakeYaml}")
    }
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation("redis.clients:jedis:${Versions.jedis}")

    implementation("com.beust:jcommander:${Versions.jCommander}")

    implementation("com.almworks.integers:integers:${Versions.almworks}")

    implementation("io.minio:minio:${Versions.minio}") {
        runtimeOnly("com.google.guava:guava:${Versions.guava}")
        runtimeOnly("com.squareup.okhttp3:okhttp:${Versions.okhttp}")
    }

    implementation("com.azure:azure-storage-blob:${Versions.azureStorage}") {
        runtimeOnly(platform("io.netty:netty-bom:${Versions.netty}"))
        runtimeOnly("io.projectreactor.netty:reactor-netty-http:${Versions.projectReactorNetty}")
    }
    implementation("com.opencsv:opencsv:${Versions.opencsv}") {
        runtimeOnly("org.apache.commons:commons-text:${Versions.apacheCommonsText}")
    }
    implementation("org.radarbase:managementportal-client:${Versions.managementPortal}")
    implementation("org.radarbase:radar-commons-kotlin:${Versions.radarCommons}")

    testImplementation("org.radarbase:radar-schemas-commons:${Versions.radarSchemas}")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${Versions.coroutines}")

    testImplementation("org.hamcrest:hamcrest:${Versions.hamcrest}")
    testImplementation("org.mockito.kotlin:mockito-kotlin:${Versions.mockitoKotlin}")
}

application {
    mainClass.set("org.radarbase.output.Application")
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
    classpath = classpath?.let { it + files("lib/PlaceHolderForPluginPath") }

    doLast {
        windowsScript.writeText(windowsScript.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins\\*"))
        unixScript.writeText(unixScript.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins/*"))
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

dockerCompose {
    waitForTcpPortsTimeout.set(Duration.ofSeconds(30))
    environment.put("SERVICES_HOST", "localhost")
    captureContainersOutputToFiles.set(project.file("build/container-logs"))
    isRequiredBy(integrationTest)
}

tasks["composeUp"].dependsOn("composePull")

tasks["check"].dependsOn(integrationTest)
