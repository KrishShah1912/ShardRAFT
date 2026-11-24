import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],

  server: {
    port: 5173,

    proxy: {
      "/node1": {
        target: "http://127.0.0.1:8081",
        changeOrigin: true,
        rewrite: (path) =>
          path.replace(/^\/node1/, ""),
      },

      "/node2": {
        target: "http://127.0.0.1:8082",
        changeOrigin: true,
        rewrite: (path) =>
          path.replace(/^\/node2/, ""),
      },

      "/node3": {
        target: "http://127.0.0.1:8083",
        changeOrigin: true,
        rewrite: (path) =>
          path.replace(/^\/node3/, ""),
      },

      "/control": {
        target: "http://127.0.0.1:8090",
        changeOrigin: true,
        rewrite: (path) =>
          path.replace(/^\/control/, ""),
      },
    },
  },
});
