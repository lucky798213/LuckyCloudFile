import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const target = process.env.API_PROXY_TARGET || env.API_PROXY_TARGET
  return {
    plugins: [react()],
    server: { proxy: target ? { '/api': { target, changeOrigin: true } } : undefined },
    preview: { proxy: target ? { '/api': { target, changeOrigin: true } } : undefined },
    test: {
      environment: 'jsdom',
      setupFiles: ['./src/test/setup.ts'],
      restoreMocks: true,
      include: ['src/**/*.test.{ts,tsx}'],
    },
  }
})
