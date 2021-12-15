pluginManagement {
    plugins {
        val kotlinVersion: String by settings
        kotlin("jvm") version kotlinVersion

        val dokkaVersion: String by settings
        id("org.jetbrains.dokka") version dokkaVersion

        val dockerComposeVersion: String by settings
        id("com.avast.gradle.docker-compose") version dockerComposeVersion

        val dependencyUpdateVersion: String by settings
        id("com.github.ben-manes.versions") version dependencyUpdateVersion

        val nexusPublishVersion: String by settings
        id("io.github.gradle-nexus.publish-plugin") version nexusPublishVersion
    }
}

rootProject.name = "radar-output-restructure"
