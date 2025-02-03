import org.gradle.api.Project

plugins {
    application
}

application {
    mainClass.set("com.ddtest.Main")
}

repositories {
    mavenCentral()
    maven { url = uri("https://nexus.rhombus.corp/repository/maven") }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

val awsJavaSdkVersion = "2.17.3"
val dashdiveSdkVersion = "1.0.10"

dependencies {
    implementation(platform("software.amazon.awssdk:bom:$awsJavaSdkVersion"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:sts")

    implementation("com.rhombus.cloud:rhombus-dashdive-collector-sdk:${dashdiveSdkVersion}")
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("org.slf4j:slf4j-api:1.7.25")
    
    runtimeOnly("org.slf4j:slf4j-simple:2.0.12")
}
