import org.gradle.api.Project

plugins {
    application
}

application {
    mainClass.set("com.ddtest.Main")
}

fun Project.containsTask(name: String): Boolean {
    return gradle.startParameter.taskNames.contains(name)
}

tasks.register("runCentralManual") {
    dependsOn("run")
}

repositories {
    if (project.containsTask("runCentralManual")) {
        val bearerToken: String = System.getenv("BEARER_TOKEN")
            ?: throw GradleException("BEARER_TOKEN environment variable is not set.")
        maven {
            name = "centralManualTesting"
            url = uri("https://central.sonatype.com/api/v1/publisher/deployments/download/")
            credentials(HttpHeaderCredentials::class) {
                name = "Authorization"
                value = "Bearer $bearerToken"
            }
            authentication {
                create<HttpHeaderAuthentication>("header")
            }
        }
    } else {
        mavenLocal()
    }
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

val awsJavaSdkVersion = "2.17.3"
val dashdiveSdkVersion_centralManualTesting = "1.0.0-rc1"
val dashdiveSdkVersion_allOtherTasks = "1.0.0-rc1"
val dashdiveSdkVersion = if (project.containsTask("runCentralManual")) {
    dashdiveSdkVersion_centralManualTesting
} else {
    dashdiveSdkVersion_allOtherTasks
}
dependencies {
    implementation(platform("software.amazon.awssdk:bom:$awsJavaSdkVersion"))
    implementation("software.amazon.awssdk:s3")

    implementation("com.dashdive:collector-sdk:$dashdiveSdkVersion")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:1.7.25")
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    runtimeOnly("org.slf4j:slf4j-simple:2.0.12")
}

task("formatSource", Exec::class) {
    commandLine("sh", "-c", "find src -name \"*.java\" -exec google-java-format -r {} +")
}

tasks.register("generateMultipartTextFile") {
    doLast {
        exec {
            commandLine("bash", "generate_multipart.sh", "src/main/resources/multipart.txt", "211000")
        }
    }
}
tasks.named("run") {
    dependsOn("generateMultipartTextFile")
}
