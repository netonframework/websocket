plugins { kotlin("multiplatform"); `maven-publish` }

// Same targets as com.netonstream:io.
kotlin {
    linuxX64(); linuxArm64()
    macosArm64(); macosX64()
    mingwX64()
    iosArm64(); iosSimulatorArm64(); iosX64()
    androidNativeArm64(); androidNativeArm32(); androidNativeX64(); androidNativeX86()

    sourceSets {
        commonMain.dependencies {
            api("com.netonstream:io:0.1.0")
            // HTTP types and the HTTP/1 head parser for the handshake (SPEC §1); the http repo is included as a build until it is published.
            api("com.netonstream:http:0.1.0")
        }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
