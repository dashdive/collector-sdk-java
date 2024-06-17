import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            // https://maven.apache.org/guides/mini/guide-naming-conventions.html
            groupId = "com.dashdive"
            artifactId = "collector-sdk"
            version = "1.0.0"

            from(components["java"])

            pom {
                name = "Dashdive Collector SDK"
                description = "The Dashdive Collector SDK makes it easy to collect cloud usage data by instrumenting Java clients for popular cloud services, such as AWS S3."
                url = "http://docs.dashdive.com/collector-sdk/"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "adamshugar"
                        name = "Adam Shugar"
                        email = "adam@dashdive.com"
                    }
                }
                scm {
                    connection = "scm:git:git://example.com/my-library.git"
                    developerConnection = "scm:git:ssh://example.com/my-library.git"
                    url = "http://docs.dashdive.com/collector-sdk/"
                }
            }
        }
    }

    repositories {
        mavenLocal()
    }
}

val awsJavaSdkVersion = "2.20.32"
dependencies {
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // https://mvnrepository.com/artifact/org.mockito/mockito-core
    testImplementation("org.mockito:mockito-core:5.10.0")

    implementation(platform("software.amazon.awssdk:bom:$awsJavaSdkVersion"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:imds")
    
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:1.7.25")
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    runtimeOnly("org.slf4j:slf4j-simple:2.0.12")

    // https://mvnrepository.com/artifact/org.apache.commons/commons-lang3
    implementation("org.apache.commons:commons-lang3:3.14.0")

    implementation(platform("com.fasterxml.jackson:jackson-bom:2.16.2"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")

    // https://mvnrepository.com/artifact/com.google.guava/guava
    implementation("com.google.guava:guava:32.1.3-jre")

    val immutablesVersion = "2.10.1"
    annotationProcessor("org.immutables:value:$immutablesVersion")
    testAnnotationProcessor("org.immutables:value:$immutablesVersion")
    implementation("org.immutables:value:$immutablesVersion")
    implementation("org.immutables:builder:$immutablesVersion")
}


fun isImplemented_ServiceClientConfig(): Boolean {
    val versionParts = awsJavaSdkVersion.split(".")
    val majorVersion = versionParts[0].toInt()
    val minorVersion = versionParts.getOrNull(1)?.toInt() ?: 0
    val patchVersion = versionParts.getOrNull(2)?.toInt() ?: 0
    return majorVersion >= 2 && minorVersion >= 20 && patchVersion >= 32
}

fun excludeConditionally(sourceSet: SourceSet, condition: () -> Boolean, vararg paths: String) {
    if (condition()) {
        sourceSet.java.exclude(*paths)
    }
}

sourceSets {
    main {
        excludeConditionally(
            sourceSet = this,
            condition = { !isImplemented_ServiceClientConfig() },
            paths = arrayOf("**/InterceptorIdempotencyTest.java")
        )
    }
    test {
        excludeConditionally(
            sourceSet = this,
            condition = { !isImplemented_ServiceClientConfig() },
            paths = arrayOf("**/InterceptorIdempotencyTest.java")
        )
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

task("formatSource", Exec::class) {
    commandLine("sh", "-c", "find src -name \"*.java\" -exec google-java-format -r {} +")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
        exceptionFormat = TestExceptionFormat.FULL
    }
    // Necessary to silence Mockito warnings: https://github.com/mockito/mockito/issues/3037
    jvmArgs("-XX:+EnableDynamicAgentLoading")
}
