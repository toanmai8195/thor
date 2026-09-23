"""ts_project dùng chung cho các layer của friend-service."""

load("@aspect_rules_ts//ts:defs.bzl", "ts_project")

_SERVICE = "//com/tm/friend-service"

def ts_layer(name, srcs = None, deps = [], visibility = None, **kwargs):
    """ts_project với tsconfig chung, sinh .d.ts cho layer khác dùng.

    Luôn thêm `@types/node` và `package.json` (tsc cần `"type": "module"` để
    biên dịch ra ESM).

    Args:
        name: tên target (thường trùng tên layer).
        srcs: file .ts; mặc định mọi *.ts trong package.
        deps: layer khác và npm package.
        visibility: ai được phụ thuộc vào layer này.
        **kwargs: truyền thẳng cho ts_project.
    """
    ts_project(
        name = name,
        srcs = srcs if srcs != None else native.glob(["*.ts"]),
        declaration = True,
        tsconfig = _SERVICE + ":tsconfig",
        visibility = visibility,
        deps = deps + [
            _SERVICE + ":node_modules/@types/node",
            _SERVICE + ":package_json",
        ],
        **kwargs
    )
