plugins {
    kotlin("jvm") version "1.9.25"
    application
}

group = "com.kafka"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:3.7.2")
    testImplementation(kotlin("test"))
}

application {
    mainClass.set("MainKt")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

sourceSets {
    val main by getting {
        kotlin.srcDirs("src")
        resources.srcDirs("resources")
    }
    val test by getting {
        kotlin.srcDirs("test")
        resources.srcDirs("testResources")
    }
}
