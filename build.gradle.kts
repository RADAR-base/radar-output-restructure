import org.radarbase.gradle.plugin.radarKotlin
import org.radarbase.gradle.plugin.radarPublishing
import java.time.Duration

plugins {
    id("application")
    alias(libs.plugins.radar.root.project)
    alias(libs.plugins.radar.dependency.management)
    alias(libs.plugins.radar.kotlin)
    alias(libs.plugins.radar.publishing)
    alias(libs.plugins.docker.compose)
}

description = "RADAR-base output restructuring"

radarRootProject {
    projectVersion.set(libs.versions.project)
    gradleVersion.set(libs.versions.gradle)
}

radarKotlin {
    javaVersion.set(libs.versions.java.get().toInt())
    kotlinVersion.set(libs.versions.kotlin)
    log4j2Version.set(libs.versions.log4j2)
    slf4jVersion.set(libs.versions.slf4j)
    sentryEnabled.set(true)
}

radarPublishing {
    val githubRepoName = "RADAR-base/radar-output-restructure"
    githubUrl.set("https://github.com/$githubRepoName.git")
    developers {
        developer {
            id.set("pvannierop")
            name.set("Pim Van Nierop")
            email.set("pim@thehyve.nl")
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

    // --- Vulnerability fixes start ---
    constraints {
        add("implementation", rootProject.libs.jackson.bom) {
            because("Force safe version of Jackson across all modules")
        }
        add("implementation", rootProject.libs.apache.commons.lang) {
            because("Force safe version of commons-lang across all modules")
        }
    }
    // --- Vulnerability fixes end ---

    api(libs.avro)
    runtimeOnly(libs.snappy.java)

    implementation(kotlin("reflect"))
    implementation(libs.kotlinx.coroutines.core)

    api(platform(libs.jackson.bom))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml) {
        runtimeOnly(libs.snakeyaml)
    }
    implementation(libs.jackson.dataformat.csv)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.jackson.datatype.jsr310)

    implementation(libs.jedis)

    implementation(libs.jcommander)

    implementation(libs.integers)

    implementation(libs.minio) {
        runtimeOnly(libs.guava)
        runtimeOnly(libs.okhttp)
    }

    implementation(libs.azure.storage.blob) {
        runtimeOnly(platform(libs.netty.bom))
        runtimeOnly(libs.reactor.netty.http)
    }
    implementation(libs.opencsv) {
        runtimeOnly(libs.apache.commons.text)
    }
    implementation(libs.managementportal.client)
    implementation(libs.radar.commons.kotlin)

    testImplementation(libs.radar.schemas.commons)
    testImplementation(libs.kotlinx.coroutines.test)

    testImplementation(libs.hamcrest)
    testImplementation(libs.mockito.kotlin)
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
