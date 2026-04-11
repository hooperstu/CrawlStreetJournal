import { defineConfig } from "@playwright/test";

/**
 * Generates the HTML bundle at ../playwright-report (Playwright v2 reporter format).
 * The Python E2E suite lives in tests/test_playwright.py; this config exists so
 * `npx playwright show-report` has a valid report directory in-repo.
 */
export default defineConfig({
  testDir: ".",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: [["html", { outputFolder: "../playwright-report", open: "never" }]],
  use: {
    baseURL: "http://127.0.0.1:5001",
    trace: "off",
  },
  webServer: {
    command: "../.venv/bin/python ../gui.py",
    url: "http://127.0.0.1:5001/",
    reuseExistingServer: true,
    timeout: 120_000,
    stdout: "pipe",
    stderr: "pipe",
  },
  projects: [{ name: "chromium", use: { channel: "chromium" } }],
});
