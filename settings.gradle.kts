pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement {
    repositories { mavenCentral() }
}
rootProject.name = "websocket-build"
include(":websocket")

// Autobahn drivers and benchmarks (SPEC §10).
include(":websocket-bench")
