def _arrow_flight_local_repository_impl(repository_ctx):
    if 'ARROW_FLIGHT_LIBRARY_PREFIX' in repository_ctx.os.environ:
        library_prefix = repository_ctx.os.environ['ARROW_FLIGHT_LIBRARY_PREFIX']
    else:
        fail("Environment Variable ARROW_FLIGHT_LIBRARY_PREFIX not found")
    print("flight library prefix in environment specified; %s"%library_prefix)

    build_file_content = """
cc_library(
    name = "arrow_flight",
    srcs = glob(["flight/lib/**/libarrow_flight*.so"]),
    visibility = ["//visibility:public"]
)"""
    print(build_file_content)

    repository_ctx.symlink(library_prefix, "./flight/lib")
    repository_ctx.file("BUILD", build_file_content)

flight_local_repository = repository_rule(
    implementation=_arrow_flight_local_repository_impl,
    local = True,
    environ = ["ARROW_FLIGHT_LIBRARY_PREFIX"])

