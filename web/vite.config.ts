import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const proxyTo = (prefix: string, port: number) => ({
  target: `http://127.0.0.1:${port}`,
  changeOrigin: true,
  rewrite: (path: string) =>
    path.slice(prefix.length) || "/",
});

export default defineConfig({
  plugins: [react()],

  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,

    watch: {
      ignored: [
        "**/playwright-multi-report/**",
        "**/test-results-multi/**",
      ],
    },

    proxy: {
      "/multi/control": proxyTo("/multi/control", 18090),
      "/node1": proxyTo("/node1", 8081),
      "/node2": proxyTo("/node2", 8082),
      "/node3": proxyTo("/node3", 8083),
      "/control": proxyTo("/control", 8090),

      "/multi/node1": proxyTo("/multi/node1", 18081),
      "/multi/node2": proxyTo("/multi/node2", 18082),
      "/multi/node3": proxyTo("/multi/node3", 18083),
      "/multi/node4": proxyTo("/multi/node4", 18084),
      "/multi/node5": proxyTo("/multi/node5", 18085),

      "/multi/gateway1": proxyTo("/multi/gateway1", 18091),
      "/multi/gateway2": proxyTo("/multi/gateway2", 18092),
    },
  },
});
