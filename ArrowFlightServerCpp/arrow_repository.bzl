def _arrow_local_repository_impl(repository_ctx):
    if 'ARROW_LIBRARY_PREFIX' in repository_ctx.os.environ:
        library_prefix = repository_ctx.os.environ['ARROW_LIBRARY_PREFIX']
    else:
        fail("Environment Variable ARROW_LIBRARY_PREFIX not found")
    print("arrow library prefix in environment specified; %s"%library_prefix)

    if 'ARROW_INCLUDE_PREFIX' in repository_ctx.os.environ:
        include_prefix = repository_ctx.os.environ['ARROW_INCLUDE_PREFIX']
    else:
        fail("Environment Variable ARROW_INCLUDE_PREFIX not found")
    print("arrow include prefix in environment specified; %s"%include_prefix)
    build_file_content = """
cc_library(
    name = "arrow",
    srcs = glob(["arrow/lib/**/libarrow*.so"]),
    hdrs = glob(["arrow/include/**"]),
    includes = ["arrow/include/"],
    visibility = ["//visibility:public"]
)"""
    print(build_file_content)

    repository_ctx.symlink(library_prefix, "./arrow/lib")
    repository_ctx.symlink(include_prefix, "./arrow/include")
    repository_ctx.file("BUILD", build_file_content)

arrow_local_repository = repository_rule(
    implementation=_arrow_local_repository_impl,
    local = True,
    environ = ["ARROW_LIBRARY_PREFIX","ARROW_INCLUDE_PREFIX"])

