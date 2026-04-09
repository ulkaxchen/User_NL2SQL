import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    watch: {
      // Linux ENOSPC workaround: avoid inotify watcher limit by polling.
      usePolling: true,
      interval: 1000,
    },
    // 与前端同源请求 /api，避免 localhost:5173 调 127.0.0.1:8000 的跨域/刷新偶发失败
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
