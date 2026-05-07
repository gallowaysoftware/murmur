import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// During `npm run dev` the UI is served at :5173 and proxies /api to the admin
// server (default :8080). The Go binary that serves production embeds dist/ and
// hosts /api itself — no proxy needed in prod.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.MURMUR_ADMIN_URL ?? 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
