plugins {
    `java-library`
    `maven-publish`
    signing
    // Maintained successor of johnrengelman/shadow; compatible with Gradle 8.x and Java 21.
    id("com.gradleup.shadow") version "8.3.5"
}

group = "com.coomia.flink"
version = "0.1.0"
description = "Apache Flink connector for TuGraph-DB over the Bolt protocol (DataStream Sink + Table/SQL)."

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    withSourcesJar()
    withJavadocJar()
}

repositories {
    // Aliyun mirror first (fast in CN), Maven Central as the canonical fallback.
    maven("https://maven.aliyun.com/repository/public")
    mavenCentral()
}

val flinkVersion = "1.20.1"
// neo4j-java-driver 4.4.x is the Bolt driver line officially verified against TuGraph-DB 4.x.
val neo4jDriverVersion = "4.4.12"
val junitVersion = "5.10.2"
val testcontainersVersion = "1.19.7"

dependencies {
    // ---- Flink: provided scope (supplied by the cluster at runtime, never shaded) ----
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-runtime:$flinkVersion")
    compileOnly("org.slf4j:slf4j-api:1.7.36")

    // ---- Runtime dependency bundled into the shaded jar ----
    api("org.neo4j.driver:neo4j-java-driver:$neo4jDriverVersion")

    // ---- Test ----
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-test-utils:$flinkVersion")
    testRuntimeOnly("org.apache.flink:flink-clients:$flinkVersion")
    testRuntimeOnly("org.slf4j:slf4j-simple:1.7.36")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.25.3")

    // Integration tests (require TUGRAPH_IT=1 and a reachable Docker daemon / TuGraph image)
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-Xlint:all,-processing")
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        charSet = "UTF-8"
        // Flink and slf4j are compileOnly; do not fail javadoc on their absence.
        addStringOption("Xdoclint:none", "-quiet")
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    // Flink on Java 17+ needs these module opens for its serializers / memory access.
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    )
    // Surface the integration-test toggles to the JVM running the tests.
    System.getenv("TUGRAPH_IT")?.let { environment("TUGRAPH_IT", it) }
    System.getenv("TUGRAPH_LIVE")?.let { environment("TUGRAPH_LIVE", it) }
}

tasks.shadowJar {
    archiveClassifier.set("")
    // Relocate the bundled Bolt driver and its transitive runtime so the connector never
    // clashes with another neo4j client present on the job / cluster classpath.
    relocate("org.neo4j.driver", "com.coomia.flink.tugraph.shaded.neo4j.driver")
    relocate("io.netty", "com.coomia.flink.tugraph.shaded.netty")
    relocate("org.reactivestreams", "com.coomia.flink.tugraph.shaded.reactivestreams")
    // Merge META-INF/services so the Table Factory SPI entry is preserved.
    mergeServiceFiles()
}

// `build` should also produce the redistributable shaded jar.
tasks.build {
    dependsOn(tasks.shadowJar)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Flink TuGraph Connector")
                description.set(project.description)
                url.set("https://github.com/coomia-ai/flink-tugraph-connector")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("spancer")
                        name.set("spancer")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/coomia-ai/flink-tugraph-connector.git")
                    developerConnection.set("scm:git:ssh://github.com/coomia-ai/flink-tugraph-connector.git")
                    url.set("https://github.com/coomia-ai/flink-tugraph-connector")
                }
            }
        }
    }
}

signing {
    // Only sign when a signing key is configured (e.g. during a release); no-op for local builds.
    isRequired = gradle.taskGraph.hasTask("publish")
    setRequired({ gradle.taskGraph.hasTask("publish") })
    sign(publishing.publications["maven"])
}
