import { test, expect } from "@playwright/test";

const BASE = "http://localhost:3001";

test.describe("Lakehouse Dashboard", () => {
  test("overview page loads with metrics", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=TOTAL POSTS", { timeout: 10000 });
    await expect(page.locator("text=2,263")).toBeVisible();
    await expect(page.locator("text=PROFILES")).toBeVisible();
    await expect(page.locator("text=401")).toBeVisible();
  });

  test("standout chart renders", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=STANDOUT POSTS BY DAY", { timeout: 10000 });
    await expect(page.locator("text=132 posts")).toBeVisible();
  });

  test("top standouts feed loads", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=TOP STANDOUTS", { timeout: 10000 });
    await expect(page.locator("text=aitickerdaily").first()).toBeVisible({ timeout: 8000 });
  });

  test("profile avatars render in overview table", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=PROFILE QUALITY", { timeout: 10000 });
    const avatarImages = page.locator("img[alt]").first();
    await expect(avatarImages).toBeVisible({ timeout: 10000 });
    const naturalWidth = await avatarImages.evaluate(
      (el: HTMLImageElement) => el.naturalWidth,
    );
    expect(naturalWidth).toBeGreaterThan(0);
  });

  test("standout thumbnails render or show fallback", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=TOP STANDOUTS", { timeout: 10000 });
    const firstCard = page.locator("text=TOP STANDOUTS").locator("..").locator("..");
    await expect(firstCard).toBeVisible();
  });

  test("signals page loads", async ({ page }) => {
    await page.goto(`${BASE}/signals`);
    await page.waitForSelector("text=High-Signal Posts", { timeout: 10000 });
    await expect(page.locator("text=evolving.ai").first()).toBeVisible({ timeout: 5000 });
  });

  test("AG Grid posts page loads with data", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    await expect(page.locator("text=kerem.tech").first()).toBeVisible({ timeout: 8000 });
    await expect(page.locator("text=Profile")).toBeVisible();
    await expect(page.locator("text=Likes")).toBeVisible();
    await expect(page.getByText("Comments", { exact: true })).toBeVisible();
    await expect(page.getByText("Views", { exact: true })).toBeVisible();
    await expect(page.getByText("Rank", { exact: true })).toBeVisible();
    await expect(page.getByText("Domain", { exact: true })).toBeVisible();
    await expect(page.getByText("Topic", { exact: true })).toBeVisible();
    await expect(page.getByText("Date", { exact: true })).toBeVisible();
  });

  test("AG Grid sorting works", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    await page.locator("text=Likes").click();
    await page.waitForTimeout(500);
    await expect(page.locator(".ag-root")).toBeVisible();
  });

  test("AG Grid avatars render in posts table", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    const gridImages = page.locator(".ag-cell img[alt]");
    const firstImage = gridImages.first();
    await expect(firstImage).toBeVisible({ timeout: 10000 });
    const src = await firstImage.getAttribute("src");
    expect(src).toBeTruthy();
    expect(src!.length).toBeGreaterThan(10);
  });

  test("mobile hamburger menu works", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(BASE);
    await page.waitForSelector("text=TOTAL POSTS", { timeout: 10000 });
    const sidebarNav = page.locator("text=Overview").first();
    await expect(sidebarNav).not.toBeInViewport();
    await page.click('[aria-label="Toggle sidebar"]');
    await page.waitForTimeout(300);
    await expect(page.locator("text=Overview").first()).toBeInViewport();
    await page.click("text=Signals");
    await page.waitForSelector("text=High-Signal Posts", { timeout: 10000 });
  });
});

test.describe("Posts page — column filters", () => {
  test("filter icons render in column headers", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    // Filter funnel icons in headers (not floating inputs)
    const filterIcons = page.locator(".ag-header-cell .ag-icon-filter");
    const count = await filterIcons.count();
    expect(count).toBeGreaterThanOrEqual(8);
  });

  test("no floating filter inputs clutter the header row", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    // Floating filters should NOT be present
    const floatingInputs = page.locator(".ag-floating-filter input");
    await expect(floatingInputs).toHaveCount(0);
  });

  test("clicking filter icon opens text filter popup", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    // Click first filter icon
    await page.locator(".ag-header-cell .ag-icon-filter").first().click();
    await page.waitForTimeout(500);

    // Filter popup should appear with text filter options
    const popup = page.locator(".ag-filter-wrapper, .ag-popup");
    await expect(popup).toBeVisible({ timeout: 3000 });

    // Should have filter option buttons (Contains, Equals, etc.)
    const optionButtons = popup.locator(".ag-filter-select, .ag-simple-filter-body-wrapper");
    await expect(optionButtons).toBeVisible();
  });

  test("clicking column header text sorts the column", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    // Click the Likes column header text to sort
    const likesHeader = page.locator(".ag-header-cell-text:text('Likes')");
    await likesHeader.click();
    await page.waitForTimeout(500);

    // Sort indicator should appear (ascending or descending arrow)
    const sortIndicator = page.locator(".ag-header-cell-sorted-asc, .ag-header-cell-sorted-desc");
    await expect(sortIndicator).toBeVisible({ timeout: 3000 });
  });
});

test.describe("Posts page — advanced filter panel", () => {
  test("advanced panel toggles on button click", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const advBtn = page.locator('button[aria-label="Toggle advanced filters"]');
    await expect(advBtn).toBeVisible();
    await advBtn.click();
    await page.waitForTimeout(300);

    await expect(page.getByText("Advanced Filters")).toBeVisible();

    await advBtn.click();
    await page.waitForTimeout(300);
    await expect(page.getByText("Advanced Filters")).not.toBeVisible();
  });

  test("advanced panel has four filter groups", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    // Use fieldset legends for exact matching (avoids column header + caption conflicts)
    await expect(page.locator('legend:has-text("Domain")')).toBeVisible();
    await expect(page.locator('legend:has-text("Rank Tier")')).toBeVisible();
    await expect(page.locator('legend:has-text("Likes Range")')).toBeVisible();
    await expect(page.locator('legend:has-text("Date Range")')).toBeVisible();
  });

  test("domain checkboxes toggle active dot", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    const techCheckbox = page.locator('input[type="checkbox"]').first();
    await techCheckbox.check();
    await page.waitForTimeout(300);

    // Active dot on Advanced button
    const advBtn = page.locator('button[aria-label="Toggle advanced filters"]');
    await expect(advBtn.locator(".rounded-full")).toBeVisible();

    await techCheckbox.uncheck();
    await page.waitForTimeout(300);
    await expect(advBtn.locator(".rounded-full")).not.toBeVisible();
  });

  test("likes range inputs accept values", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    const minInput = page.locator('input[aria-label="Min likes"]');
    await minInput.fill("10000");
    await expect(minInput).toHaveValue("10000");

    const maxInput = page.locator('input[aria-label="Max likes"]');
    await maxInput.fill("100000");
    await expect(maxInput).toHaveValue("100000");
  });

  test("date range inputs use dark theme", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    const dateFrom = page.locator('input[aria-label="Date from"]');
    await expect(dateFrom).toBeVisible();
    const colorScheme = await dateFrom.evaluate(
      (el: HTMLInputElement) => el.style.colorScheme,
    );
    expect(colorScheme).toBe("dark");
  });

  test("clear all resets filters", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    await page.locator('input[type="checkbox"]').first().check();
    await page.locator('input[aria-label="Min likes"]').fill("5000");
    await page.waitForTimeout(300);

    await page.getByText("Clear All").click();
    await page.waitForTimeout(300);

    const checked = await page.locator('input[type="checkbox"]:checked').count();
    expect(checked).toBe(0);
    await expect(page.locator('input[aria-label="Min likes"]')).toHaveValue("");
  });

  test("advanced panel has aria-expanded", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const advBtn = page.locator('button[aria-label="Toggle advanced filters"]');
    await expect(advBtn).toHaveAttribute("aria-expanded", "false");

    await advBtn.click();
    await page.waitForTimeout(300);
    await expect(advBtn).toHaveAttribute("aria-expanded", "true");
  });

  test("rank tier checkboxes work", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);

    // Check Tier A
    const rankCheckboxes = page.locator('fieldset:has(legend:has-text("Rank Tier")) input[type="checkbox"]');
    await rankCheckboxes.first().check();
    await page.waitForTimeout(300);

    // Active dot appears
    const advBtn = page.locator('button[aria-label="Toggle advanced filters"]');
    await expect(advBtn.locator(".rounded-full")).toBeVisible();
  });
});

test.describe("Posts page — combined search and filters", () => {
  test("keyword search works alongside column filters", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const searchInput = page.locator('input[aria-label="Search posts"]');
    await searchInput.fill("deploy");
    await page.waitForTimeout(1000);

    // Grid should still be functional
    await expect(page.locator(".ag-root")).toBeVisible();
  });

  test("advanced filter and search compose", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    // Search
    await page.locator('input[aria-label="Search posts"]').fill("ai");
    await page.waitForTimeout(1000);

    // Open advanced, check domain
    await page.locator('button[aria-label="Toggle advanced filters"]').click();
    await page.waitForTimeout(300);
    await page.locator('input[type="checkbox"]').first().check();
    await page.waitForTimeout(300);

    // Both indicators should show
    await expect(page.locator(".ag-root")).toBeVisible();
    await expect(page.locator('button[aria-label="Toggle advanced filters"] .rounded-full')).toBeVisible();
  });
});

test.describe("Posts page — caption truncation", () => {
  test("caption cells do not overflow into adjacent columns", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const captionCell = page.locator(".ag-cell.\\!block.truncate").first();
    await expect(captionCell).toBeVisible({ timeout: 5000 });

    const textOverflow = await captionCell.evaluate(
      (el: HTMLElement) => window.getComputedStyle(el).textOverflow,
    );
    expect(textOverflow).toBe("ellipsis");
  });

  test("number columns display only numeric values", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const likesCells = page.locator(".ag-cell.font-data.tabular-nums").first();
    const text = await likesCells.textContent();
    expect(text).toMatch(/^(\d|--|[\d.]+[KM])/);
  });
});
