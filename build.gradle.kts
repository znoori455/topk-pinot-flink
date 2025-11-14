plugins {
    kotlin("jvm") version "1.9.21"
    application
    id("com.github.ben-manes.versions") version "0.51.0"
//    id("com.github.johnrengelman.shadow") version "8.1.1"


//    kotlin("plugin.spring") version "1.9.25"
//    id("org.springframework.boot") version "3.5.7"
//    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.restaurant.topk"
//version = "1.0.0"
//description = "Demo project for Spring Boot"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

//kotlin {
//
//}

//tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
//kotlinOptions {
//    jvmTarget = "21"
//}
//}

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // Flink
    implementation("org.apache.flink:flink-streaming-java:2.1.0")
    implementation("org.apache.flink:flink-clients:2.1.0")
    implementation("org.apache.flink:flink-connector-kafka:4.0.1-2.0")
    implementation("org.apache.flink:flink-connector-base:2.1.0")
    implementation("org.apache.flink:flink-table-api-java-bridge:2.1.0")
    implementation("org.apache.flink:flink-json:2.1.0")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.5.1")

    // Ktor for REST API
    implementation("io.ktor:ktor-server-core:2.3.5")
    implementation("io.ktor:ktor-server-netty:2.3.5")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.5")
    implementation("io.ktor:ktor-serialization-jackson:2.3.5")

    // HTTP Client for Pinot
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // Jackson for JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
//    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
// Use a recent, stable version

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.11")

    // Testing
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.apache.flink:flink-test-utils:2.1.0")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

application {
    mainClass.set("com.restaurant.topk.flink.TopKStreamingJob.kt")
}

//tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
//    archiveBaseName.set("restaurant-topk")
//    archiveClassifier.set("")
//    archiveVersion.set("")
//}



tasks.register<Jar>("shadowJar") {
    archiveBaseName.set("restaurant-topk")
    from(sourceSets.main.get().output)

    // Include runtime dependencies
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    // Optional: merge META-INF/services
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
//
//    manifest {
//        attributes["Main-Class"] = "com.restaurant.topk.generator.DataGeneratorKt"
//    }
}


// Define multiple JAR tasks with different main classes
tasks.register<Jar>("shadowJarApi") {
    archiveBaseName.set("restaurant-topk-api")
    from(sourceSets.main.get().output)

    manifest {
        attributes["Main-Class"] = "com.restaurant.topk.api.TopKControllerKt"
    }

    // Include runtime dependencies
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    // Optional: merge META-INF/services
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.register<Jar>("shadowJarGenerator") {
    archiveBaseName.set("restaurant-topk-generator")
    from(sourceSets.main.get().output)

    manifest {
        attributes["Main-Class"] = "com.restaurant.topk.generator.DataGeneratorKt"
    }

    // Include runtime dependencies
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })

    // Optional: merge META-INF/services
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}


