plugins { kotlin("multiplatform"); `maven-publish` }

// Same targets as com.netonstream:io, the JVM included (desktop and Android apps): bytecode 1.8, API 2.2 and stdlib
// 2.2.21 so Android hosts on Kotlin 2.1 can use it (as neton-io and http).
kotlin {
    coreLibrariesVersion = "2.2.21"
    jvm {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
            apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_2)
        }
    }
    linuxX64(); linuxArm64()
    macosArm64(); macosX64()
    mingwX64()
    iosArm64(); iosSimulatorArm64(); iosX64()
    androidNativeArm64(); androidNativeArm32(); androidNativeX64(); androidNativeX86()

    sourceSets {
        commonMain.dependencies {
            api("com.netonstream:io:0.3.2")
            // HTTP types and the HTTP/1 head parser for the handshake (SPEC §1).
            api("com.netonstream:http:0.2.0")
        }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
