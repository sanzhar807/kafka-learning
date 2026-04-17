plugins {
    java
    application                                    // FIX: нужен для gradle run
    id("com.google.protobuf") version "0.9.4"
}

group = "com.kafkalearn"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

// FIX: без этого gradle run не знает какой класс запускать
application {
    mainClass.set("com.kafkalearn.Main")
}

repositories {
    mavenCentral()
}

dependencies {
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.6.1")

    // Protobuf
    implementation("com.google.protobuf:protobuf-java:3.25.1")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.1"
    }
}

tasks.test {
    useJUnitPlatform()
}
