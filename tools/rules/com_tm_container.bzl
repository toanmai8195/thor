"""Macro build binary + OCI image cho từng ngôn ngữ (theo vision).

Mỗi macro sinh cùng một bộ target:

    //path/to/app:<name>          binary chạy local (bazel run)
    //path/to/app:<name>_image    oci_image
    //path/to/app:<name>_docker   load image vào Docker local
    //path/to/app:<name>_push     push image (chỉ khi truyền `repository`)

Image tag: com.tm.{go,js}.<name>:v1.0.0

Build image đúng kiến trúc máy chạy container:

    bazel run --config=linux-arm64 //com/tm/friend-service/src:friend_service_docker
"""

load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_image_layer", "js_library")
load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@rules_go//go:def.bzl", "go_binary")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load", "oci_push")
load("@tar.bzl", "tar")

DEFAULT_IMAGE_TAG = "v1.0.0"

def _container_targets(
        name,
        base,
        repo_tag,
        tar_srcs = [],
        layers = [],
        entrypoint = None,
        cmd = None,
        env = None,
        workdir = None,
        exposed_ports = [],
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """Sinh image / docker load / push.

    `tar_srcs` được đóng thành một layer `<name>_tar`; `layers` là các layer tar
    dựng sẵn (vd từ js_image_layer), thêm sau layer đó.
    """

    tars = list(layers)
    if tar_srcs:
        tar(
            name = name + "_tar",
            srcs = tar_srcs,
            out = name + "_layer.tar",
            visibility = visibility,
        )
        tars = [":" + name + "_tar"] + tars

    oci_image(
        name = name + "_image",
        base = base,
        cmd = cmd,
        entrypoint = entrypoint,
        env = env,
        exposed_ports = exposed_ports,
        tars = tars,
        visibility = visibility,
        workdir = workdir,
    )

    oci_load(
        name = name + "_docker",
        image = ":" + name + "_image",
        repo_tags = ["%s:%s" % (repo_tag, image_tag)],
        visibility = visibility,
    )

    if repository:
        oci_push(
            name = name + "_push",
            image = ":" + name + "_image",
            remote_tags = [image_tag],
            repository = repository,
            visibility = visibility,
        )

# =============================================================================
# GO
# =============================================================================

def com_tm_go_image(
        name,
        package_name,
        embed,
        data = [],
        args = [],
        exposed_ports = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """go_binary (static, CGO off) + OCI image trên distroless.

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        embed: go_library chứa `package main` (thường do gazelle sinh).
        data: file runtime, cũng được bake vào image.
        args: tham số mặc định khi chạy local.
        exposed_ports: port expose trong container.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`; bỏ trống để không sinh target push.
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    go_binary(
        name = name,
        args = args,
        data = data,
        embed = embed,
        pure = "on",
        static = "on",
        visibility = visibility,
    )

    # rules_go đặt binary ở <name>_/<name>; copy ra path ổn định cho entrypoint.
    copy_file(
        name = name + "_bin",
        src = ":" + name,
        out = "bin/" + name,
        is_executable = True,
    )

    _container_targets(
        name = name,
        base = "@distroless_base",
        entrypoint = ["/%s/bin/%s" % (package_name, name)],
        env = env,
        exposed_ports = exposed_ports,
        image_tag = image_tag,
        repo_tag = "com.tm.go.%s" % name,
        repository = repository,
        tar_srcs = [":" + name + "_bin"] + data,
        visibility = visibility,
    )

# =============================================================================
# NODE.JS
# =============================================================================

_JS_ROOT = "/app"

def com_tm_js_image(
        name,
        package_name,
        entry_point,
        srcs,
        deps = [],
        data = [],
        args = [],
        exposed_ports = [],
        env = None,
        repository = None,
        image_tag = DEFAULT_IMAGE_TAG,
        visibility = ["//visibility:public"]):
    """js_library + js_binary + OCI image (Node.js toolchain nằm trong image).

    Sinh thêm `<name>_lib` để js_test có thể depend.

    Node binary lấy từ toolchain của rules_nodejs theo platform đích, nên image
    cần build với `--config=linux-{arm64,amd64}`.

    Args:
        name: tên gốc cho mọi target.
        package_name: truyền `package_name()` từ BUILD file.
        entry_point: file JS chạy đầu tiên, phải cùng package, vd "server.js".
        srcs: source JS của package chứa entry point.
        deps: js_library của các layer khác, npm package
            (vd "//com/tm/friend-service:node_modules/express") và target chứa
            `package.json` (cần cho `"type": "module"`).
        data: file runtime khác.
        args: tham số mặc định khi chạy local.
        exposed_ports: port expose trong container.
        env: biến môi trường cho container.
        repository: registry cho `<name>_push`; bỏ trống để không sinh target push.
        image_tag: tag image.
        visibility: visibility của target sinh ra.
    """

    lib_name = name + "_lib"

    js_library(
        name = lib_name,
        srcs = srcs,
        data = data,
        visibility = visibility,
        deps = deps,
    )

    js_binary(
        name = name,
        args = args,
        data = [":" + lib_name],
        entry_point = entry_point,
        visibility = visibility,
    )

    # Layer: node toolchain, npm package store, node_modules, app
    js_image_layer(
        name = name + "_layers",
        binary = ":" + name,
        root = _JS_ROOT,
        visibility = visibility,
    )

    bin_path = "%s/%s/%s" % (_JS_ROOT, package_name, name)

    _container_targets(
        name = name,
        base = "@node_base",
        entrypoint = [bin_path],
        env = env,
        exposed_ports = exposed_ports,
        image_tag = image_tag,
        layers = [":" + name + "_layers"],
        repo_tag = "com.tm.js.%s" % name,
        repository = repository,
        visibility = visibility,
        # js_binary launcher yêu cầu cwd là runfiles root
        workdir = bin_path + ".runfiles/_main",
    )
