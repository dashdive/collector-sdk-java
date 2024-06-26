import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import com.vanniktech.maven.publish.SonatypeHost
import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar

plugins {
    `java-library`
    // https://central.sonatype.org/publish/publish-portal-gradle/
    id("com.vanniktech.maven.publish") version "0.28.0"
}

val dashdiveCurrentVersion = "1.0.0-rc1"

mavenPublishing {
    configure(JavaLibrary(
        javadocJar = JavadocJar.Javadoc(),
        sourcesJar = true,
    ))

    coordinates("com.dashdive", "collector-sdk", dashdiveCurrentVersion)

    pom {
        name = "Dashdive Collector SDK"
        description = "The Dashdive Collector SDK makes it easy to collect cloud usage data by instrumenting Java clients for popular cloud services, such as AWS S3."
        url = "http://docs.dashdive.com/collector-sdk/"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "http://www.apache.org/licenses/LICENSE-2.0.txt"
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
            url = "http://docs.dashdive.com/collector-sdk/"
            connection = "scm:git:git://github.com/dashdive/collector-sdk-java.git"
            developerConnection = "scm:git:ssh://github.com/dashdive/collector-sdk-java.git"
        }
    }

    // By default, the release step (which comes after the publish step) is not handled by the plugin
    // when running `publishToMavenCentral` (contrast against `publishAndReleaseToMavenCentral`).
    // See: https://central.sonatype.org/publish/publish-portal-api/#uploading-a-deployment-bundle
    // See: https://vanniktech.github.io/gradle-maven-publish-plugin/central/#publishing-releases
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    gradle.taskGraph.whenReady {
        if (allTasks.none { it.name == "publishToMavenLocal" }) {
            signAllPublications()
        }
    }
}
tasks.named("generateMetadataFileForMavenPublication").configure {
    dependsOn("plainJavadocJar")
}

tasks.javadoc {
    dependsOn("compileJava")
    
    destinationDir = file("build/docs/javadoc")
    include("com/dashdive/*.java")
    exclude("com/dashdive/internal/**")

    source = files(
        sourceSets["main"].java.srcDirs,
        file("build/generated/sources/annotationProcessor/java/main")
    ).asFileTree

    title = "Dashdive Collector SDK - $dashdiveCurrentVersion"
    
    options {
        // Workaround for: https://github.com/gradle/gradle/issues/7038
        // See: https://stackoverflow.com/a/74219033/14816795
        require(this is StandardJavadocDocletOptions)
        links(
            "https://docs.oracle.com/en/java/javase/11/docs/api/",
            "https://sdk.amazonaws.com/java/api/latest/")
    }
}
tasks.named("build") {
    dependsOn("javadoc")
}

repositories {
    mavenCentral()
}

val awsJavaSdkVersion = "2.17.3"
dependencies {
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // https://mvnrepository.com/artifact/org.mockito/mockito-core
    testImplementation("org.mockito:mockito-core:5.10.0")

    implementation(platform("software.amazon.awssdk:bom:$awsJavaSdkVersion"))
    implementation("software.amazon.awssdk:s3")
    
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
        languageVersion = JavaLanguageVersion.of(11)
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
