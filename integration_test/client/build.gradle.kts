plugins {
    application
}

application {
    mainClass.set("com.ddtest.Main")
}

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

val awsJavaSdkVersion = "2.17.3"
dependencies {
    implementation(platform("software.amazon.awssdk:bom:$awsJavaSdkVersion"))
    implementation("software.amazon.awssdk:s3")

    implementation("com.dashdive:collector-sdk:1.0.0")

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
