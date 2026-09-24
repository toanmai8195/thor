# Build static (Vite) → phục vụ bằng Nginx, cùng Nginx reverse-proxy /api/* sang backend.
#   docker build -t com.tm.web.app:v1.0.0 .      (hoặc: cd ../server/infra && docker compose up -d --build app)

FROM node:22-alpine AS build
WORKDIR /app
RUN npm install -g pnpm@11.18.0
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile
COPY . .
RUN pnpm build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
