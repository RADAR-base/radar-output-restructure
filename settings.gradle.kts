pluginManagement {
    resolutionStrategy {
        eachPlugin {
            if (requested.id.id == "org.jetbrains.kotlin.jvm" ) {
                val kotlinVersion: String by settings
                useModule("org.jetbrains.kotlin:kotlin-gradle-plugin:$kotlinVersion")
            }
        }
    }
}

rootProject.name = "radar-output-restructure"

include(":radar-hdfs-restructure")
