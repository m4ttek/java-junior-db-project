/*
 * This file was generated by the Gradle 'init' task.
 *
 * This is a general purpose Gradle build.
 * To learn more about Gradle by exploring our Samples at https://docs.gradle.org/8.2/samples
 */
plugins {
    id("java")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(20))
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("--enable-preview")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.h2database:h2:2.2.220")

    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
}
