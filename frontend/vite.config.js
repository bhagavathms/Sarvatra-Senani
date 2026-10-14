import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server proxies REST + WebSocket traffic to the FastAPI backend,
// so the dashboard can use same-origin relative URLs.
const BACKEND = process.env.SENANI_BACKEND || "http://127.0.0.1:8000"; // not "localhost": Node may resolve it to ::1

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: BACKEND, changeOrigin: true },
      "/ws": { target: BACKEND.replace(/^http/, "ws"), ws: true },
    },
  },
});
