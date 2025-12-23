
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "resources",
    srcs = ["action.py"],
)

cc_binary(
    name = "arrow_flight_server",
    srcs = ["server.cpp"],
    deps = ["@pyarrow//:arrow", "@python3//:python3",],
    data = [":resources"]
)
