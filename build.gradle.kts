import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.time.Duration

plugins {
    kotlin("jvm")
    application
    `maven-publish`
    signing
    id("org.jetbrains.dokka")
    id("com.avast.gradle.docker-compose")
    id("com.github.ben-manes.versions")
    id("io.github.gradle-nexus.publish-plugin")
    id("org.jlleitschuh.gradle.ktlint") version "11.0.0"
}

group = "org.radarbase"
version = "2.3.3"

repositories {
    mavenCentral()
}

description = "RADAR-base output restructuring"
val website = "https://radar-base.org"
val githubRepoName = "RADAR-base/radar-output-restructure"
val githubUrl = "https://github.com/$githubRepoName"
val issueUrl = "$githubUrl/issues"

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
    val snappyVersion: String by project
    runtimeOnly("org.xerial.snappy:snappy-java:$snappyVersion")

    implementation(kotlin("reflect"))
    val coroutinesVersion: String by project
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    val jacksonVersion: String by project
    api(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml") {
        val snakeYamlVersion: String by project
        runtimeOnly("org.yaml:snakeyaml:$snakeYamlVersion")
    }
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    val jedisVersion: String by project
    implementation("redis.clients:jedis:$jedisVersion")

    val jCommanderVersion: String by project
    implementation("com.beust:jcommander:$jCommanderVersion")

    val almworksVersion: String by project
    implementation("com.almworks.integers:integers:$almworksVersion")

    val minioVersion: String by project
    implementation("io.minio:minio:$minioVersion") {
        val guavaVersion: String by project
        runtimeOnly("com.google.guava:guava:$guavaVersion")

        val okhttpVersion: String by project
        runtimeOnly("com.squareup.okhttp3:okhttp:$okhttpVersion")
    }

    val azureStorageVersion: String by project
    implementation("com.azure:azure-storage-blob:$azureStorageVersion") {
        val nettyVersion: String by project
        runtimeOnly(platform("io.netty:netty-bom:$nettyVersion"))
        val projectReactorNettyVersion: String by project
        runtimeOnly("io.projectreactor.netty:reactor-netty-http:$projectReactorNettyVersion")
    }
    val opencsvVersion: String by project
    implementation("com.opencsv:opencsv:$opencsvVersion") {
        val apacheCommonsTextVersion: String by project
        runtimeOnly("org.apache.commons:commons-text:$apacheCommonsTextVersion")
    }

    val slf4jVersion: String by project
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    val log4jVersion: String by project
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-jul:$log4jVersion")

    val radarSchemasVersion: String by project
    testImplementation("org.radarbase:radar-schemas-commons:$radarSchemasVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")

    val junitVersion: String by project
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.hamcrest:hamcrest:2.2")
    val mockitoKotlinVersion: String by project
    testImplementation("org.mockito.kotlin:mockito-kotlin:$mockitoKotlinVersion")

    val dokkaVersion: String by project
    dokkaHtmlPlugin("org.jetbrains.dokka:kotlin-as-java-plugin:$dokkaVersion")

    val jsoupVersion: String by project
    dokkaPlugin("org.jsoup:jsoup:$jsoupVersion")
    dokkaRuntime("org.jsoup:jsoup:$jsoupVersion")
    dokkaPlugin(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    dokkaRuntime(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
}

application {
    mainClass.set("org.radarbase.output.Application")
    applicationDefaultJvmArgs = listOf(
        "-Djava.security.egd=file:/dev/./urandom",
        "-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager",
    )
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

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "17"
        apiVersion = "1.6"
        languageVersion = "1.6"
        freeCompilerArgs = listOf("-opt-in=kotlin.RequiresOptIn")
    }
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

tasks.withType<Tar> {
    compression = Compression.GZIP
    archiveExtension.set("tar.gz")
}

tasks.withType<Jar> {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        )
    }
}

tasks.startScripts {
    classpath = classpath?.let { it + files("lib/PlaceHolderForPluginPath") }

    doLast {
        windowsScript.writeText(windowsScript.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins\\*"))
        unixScript.writeText(unixScript.readText().replace("PlaceHolderForPluginPath", "radar-output-plugins/*"))
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
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
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
                    connection.set("scm:git:$githubUrl")
                    url.set(githubUrl)
                }
            }
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
    onlyIf { gradle.taskGraph.hasTask(project.tasks["publish"]) }
}

fun Project.propertyOrEnv(propertyName: String, envName: String): String? {
    return if (hasProperty(propertyName)) {
        property(propertyName)?.toString()
    } else {
        System.getenv(envName)
    }
}

nexusPublishing {
    repositories {
        sonatype {
            username.set(propertyOrEnv("ossrh.user", "OSSRH_USER"))
            password.set(propertyOrEnv("ossrh.password", "OSSRH_PASSWORD"))
        }
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

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
        exceptionFormat = FULL
    }
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

fun isNonStable(version: String): Boolean {
    val stableKeyword = listOf("RELEASE", "FINAL", "GA", "JRE").any { version.toUpperCase().contains(it) }
    val regex = "^[0-9,.v-]+(-r)?$".toRegex()
    val isStable = stableKeyword || regex.matches(version)
    return isStable.not()
}

tasks.named<DependencyUpdatesTask>("dependencyUpdates").configure {
    rejectVersionIf {
        isNonStable(candidate.version)
    }
}

ktlint {
    version.set("0.45.2")
    disabledRules.set(setOf("no-wildcard-imports"))
}

tasks.wrapper {
    gradleVersion = "7.6"
}
