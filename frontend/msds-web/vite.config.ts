import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// 개발 중에는 /api를 로컬 gateway(8080)로 넘긴다
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": "http://localhost:8080" },
  },
});
