pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement {
    repositories { mavenLocal(); mavenCentral() }
}
rootProject.name = "websocket-build"
include(":websocket")

// com.netonstream:http from the sibling repo until it is published.
includeBuild("../http")

// Autobahn drivers and benchmarks (SPEC §10).
include(":websocket-bench")
