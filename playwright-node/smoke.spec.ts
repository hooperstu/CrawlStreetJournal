import { test, expect } from "@playwright/test";

/**
 * Smoke check that the CSJ Flask GUI answers on BASE_URL (default 127.0.0.1:5001).
 * Start the app first: source .venv/bin/activate && python3 gui.py
 */
test.describe("CSJ GUI smoke", () => {
  test("homepage responds", async ({ page }) => {
    const resp = await page.goto("/", { waitUntil: "domcontentloaded" });
    expect(resp, "GET / should return a status").toBeTruthy();
    expect(resp!.status(), "GET / status").toBeLessThan(500);
    await expect(page.locator("h1")).toBeVisible();
  });
});
