# thor

| Thư mục | Nội dung | Build |
|---|---|---|
| [`com/tm/server/`](com/tm/server/README.md) | Backend: friend-service (Node.js), event-gateway, friend-simulator, phonebook-service (Go), infra (Docker Compose), thiết kế StarRocks ([friend network](com/tm/server/README.md), [phonebook](com/tm/server/PHONEBOOK.md)) | Bazel — module root là `com/tm/server`, mọi lệnh `bazel` chạy trong thư mục này |
| [`com/tm/app/`](com/tm/app/) | Web app (React + TypeScript + Vite + Dexie): danh bạ local, đồng bộ, danh bạ server, friend network | pnpm, độc lập với Bazel |

```bash
cd com/tm/server
bazel test //...                                  # toàn bộ test backend
cd infra && docker compose up -d                  # stack local
```
