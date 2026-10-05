import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// SPA build -> dist/, served by the FastAPI backend (SPAStaticFiles).
// Dev server proxies /api to the local uvicorn backend on :8000.
export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": path.resolve(__dirname, "./src") } },
  server: { port: 5173, proxy: { "/api": "http://localhost:8000" } },
  build: { outDir: "dist", emptyOutDir: true },
});
