load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@tar.bzl", "mutate", "tar")

def com_tm_go_image(name, package_name, srcs, deps = [], importpath = "", exposedPorts = [], visibility = ["//visibility:public"], data = [], args = []):
    """
    Builds a Go binary, packages it into a tarball, and creates an OCI image and docker load target.

    Args:
        name: The base name for all targets (binary, tar, image, docker load).
        srcs: List of Go source files for the library.
        deps: List of Go dependencies.
        importpath: Go import path for the library.
        exposedPorts: List of ports to expose in the container (as strings).
        visibility: Visibility of the generated targets.
        package_name: Package name of build file.
        data: List of data files available at runtime for the local binary.
        args: Default arguments passed to the local binary.
    """

    lib_name = name + "_lib"
    local_bin_name = name
    docker_bin_name = name + "_docker_bin"
    tar_name = name + "_tar"
    image_name = name + "_image"
    docker_name = name + "_docker"

    go_library(
        name = lib_name,
        srcs = srcs,
        importpath = importpath,
        visibility = visibility,
        deps = deps,
    )

    # Binary cho Docker: linux/amd64, không cần pure/static flag
    go_binary(
        name = docker_bin_name,
        embed = [":" + lib_name],
        goos = "linux",
        goarch = "amd64",
    )

    # Binary cho local run
    go_binary(
        name = local_bin_name,
        embed = [":" + lib_name],
        data = data,
        args = args,
        pure = "on",
        static = "on",
    )

    tar(
        name = tar_name,
        srcs = [":" + docker_bin_name],
        out = name + ".tar",
        mutate = mutate(
            strip_prefix = package_name + "/" + docker_bin_name + "_",
        ),
    )

    oci_image(
        name = image_name,
        base = "@distroless_base",
        entrypoint = ["/" + docker_bin_name],
        tars = [":" + tar_name],
        exposed_ports = exposedPorts,
    )

    oci_load(
        name = docker_name,
        image = ":" + image_name,
        repo_tags = ["com.tm.go.%s:v1.0.0" % name],
    )
