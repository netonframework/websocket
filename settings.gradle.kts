pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement {
    repositories {
        // -PreleaseRepositories=<dir>,<dir>…: unreleased dependencies from local Maven repositories (a release
        // candidate's, in CI), ahead of Maven Central.
        providers.gradleProperty("releaseRepositories").orNull?.split(",")?.forEach { path ->
            maven { url = uri(path) }
        }
        mavenCentral()
    }
}
rootProject.name = "websocket-build"
include(":websocket")

// Autobahn drivers and benchmarks (SPEC §10).
include(":websocket-bench")
