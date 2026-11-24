import {
  defineConfig,
  devices,
} from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",

  timeout: 60_000,

  expect: {
    timeout: 10_000,
  },

  fullyParallel: false,
  workers: 1,
  retries: 0,

  reporter: [
    ["list"],
    [
      "html",
      {
        outputFolder:
          "playwright-report",
        open: "never",
      },
    ],
  ],

  use: {
    baseURL:
      "http://127.0.0.1:5173",

    trace:
      "retain-on-failure",

    screenshot:
      "only-on-failure",

    video:
      "retain-on-failure",

    ...devices[
      "Desktop Chrome"
    ],
  },

  webServer: [
    {
      command:
        "../scripts/e2e-supervisor.sh",

      url:
        "http://127.0.0.1:8090/health",

      timeout: 60_000,

      reuseExistingServer: false,

      stdout: "pipe",
      stderr: "pipe",
    },
    {
      command:
        "npm run dev -- --host 127.0.0.1",

      url:
        "http://127.0.0.1:5173",

      timeout: 30_000,

      reuseExistingServer: false,

      stdout: "pipe",
      stderr: "pipe",
    },
  ],

  outputDir:
    "test-results",
});
