pluginManagement {
    repositories {
        // Aliyun mirror first for faster resolution in CN networks; falls back to the
        // canonical Gradle Plugin Portal and Maven Central for everything else.
        maven("https://maven.aliyun.com/repository/gradle-plugin")
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "flink-tugraph-connector"
