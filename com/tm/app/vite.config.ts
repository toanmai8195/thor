import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

// Gọi API qua proxy của dev server (cùng origin, không cần CORS):
//   /api/phonebook/* → phonebook-service (mặc định http://localhost:3100)
//   /api/friend/*    → friend-service    (mặc định http://localhost:3000)
const phonebookUrl = process.env.PHONEBOOK_URL ?? 'http://localhost:3100';
const friendUrl = process.env.FRIEND_URL ?? 'http://localhost:3000';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api/phonebook': { target: phonebookUrl, changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/phonebook/, '') },
      '/api/friend': { target: friendUrl, changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/friend/, '') },
    },
  },
  test: { environment: 'node' },
});
