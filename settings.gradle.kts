pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement {
    repositories { mavenLocal(); mavenCentral() }
}
rootProject.name = "websocket-build"
include(":websocket")
