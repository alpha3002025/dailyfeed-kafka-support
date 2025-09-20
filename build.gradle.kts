plugins {
	java
	id("org.springframework.boot") version "3.5.5"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "click.dailyfeed"
version = "0.0.1-SNAPSHOT"
description = "kafka module 😎"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(17)
	}
}

configurations {
	compileOnly {
		extendsFrom(configurations.annotationProcessor.get())
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation(":dailyfeed-code")

	// spring
	implementation("org.springframework.boot:spring-boot-starter")
	// implementation("org.springframework.boot:spring-boot-starter-json")
	implementation("org.springframework.kafka:spring-kafka")

	// lombok
//	compileOnly("org.projectlombok:lombok")
//	annotationProcessor("org.projectlombok:lombok")

	// test
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
}