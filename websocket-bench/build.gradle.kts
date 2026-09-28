plugins { kotlin("multiplatform") }

// Autobahn|Testsuite drivers (SPEC §10 step 4) and benchmarks: not published.
kotlin {
    listOf(linuxX64(), linuxArm64(), macosArm64()).forEach { target ->
        target.binaries {
            executable("autobahnServer") { entryPoint = "neton.websocket.bench.autobahnServerMain" }
            executable("autobahnClient") { entryPoint = "neton.websocket.bench.autobahnClientMain" }
        }
    }
    sourceSets {
        commonMain.dependencies { implementation(project(":websocket")) }
    }
}
