/*
 * Copyright 2022 Dmitry Barashev, JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.0"
    java
    application
}

group = "net.barashev.dbi2022"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.7.3")
    implementation("net.datafaker:datafaker:1.9.0")
    implementation("com.github.ajalt.clikt:clikt:4.2.0")
    testImplementation(kotlin("test"))
}

val systemProps by extra {arrayOf("cache.impl", "cache.size", "sort.impl", "hash.impl", "index.impl", "index.method", "optimizer.impl")}

tasks.test {
    useJUnitPlatform()
    systemProperties = System.getProperties().mapKeys { it.key.toString() }.filterKeys { it in systemProps }
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

application {
    mainClass.set("net.barashev.dbi2023.app.MainKt")
    applicationDefaultJvmArgs = System.getProperties().filterKeys { it in systemProps }.map { "-D${it.key}=${it.value}"}
}
