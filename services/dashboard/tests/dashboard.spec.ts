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

  test("hot posts feed loads", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=HOT POSTS", { timeout: 10000 });
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

  test("hot post thumbnails render or show fallback", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=HOT POSTS", { timeout: 10000 });
    const firstCard = page.locator("text=HOT POSTS").locator("..").locator("..");
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
    await expect(page.getByText("Platform", { exact: true })).toBeVisible();
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

test.describe("Posts page — sorting only (no per-column filters)", () => {
  test("no filter chevrons render in column headers", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    // AG Grid always renders a hidden filter slot; with no column filter it
    // stays `.ag-hidden` (display:none), so only assert no *visible* chevrons.
    await expect(page.locator(".ag-header-cell .ag-icon-filter:visible")).toHaveCount(0);
  });

  test("no floating filter inputs clutter the header row", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    const floatingInputs = page.locator(".ag-floating-filter input");
    await expect(floatingInputs).toHaveCount(0);
  });

  test("clicking column header text sorts the column", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const likesHeader = page.locator(".ag-header-cell-text:text('Likes')");
    await likesHeader.click();
    await page.waitForTimeout(500);

    const sortIndicator = page.locator(".ag-header-cell-sorted-asc, .ag-header-cell-sorted-desc");
    await expect(sortIndicator).toBeVisible({ timeout: 3000 });
  });
});

test.describe("Posts page — Filter modal", () => {
  test("Filter button opens a modal with criteria", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const filterBtn = page.locator('button[aria-label="Open filters"]');
    await expect(filterBtn).toBeVisible();
    await expect(filterBtn).toContainText("Filter");
    await filterBtn.click();
    await page.waitForTimeout(300);

    await expect(page.getByText("Filter Criteria")).toBeVisible();

    await page.locator('button[aria-label="Close filters"]').click();
    await page.waitForTimeout(300);
    await expect(page.getByText("Filter Criteria")).not.toBeVisible();
  });

  test("modal exposes the full filter criteria set", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Open filters"]').click();
    await page.waitForTimeout(300);

    await expect(page.locator('legend:has-text("Platform")')).toBeVisible();
    await expect(page.locator('legend:has-text("Domain")')).toBeVisible();
    await expect(page.locator('legend:has-text("Rank Tier")')).toBeVisible();
    await expect(page.locator('legend:has-text("EDU")')).toBeVisible();
    await expect(page.locator('legend:has-text("ACT")')).toBeVisible();
    await expect(page.locator('legend:has-text("Likes Range")')).toBeVisible();
    await expect(page.locator('legend:has-text("Comments Range")')).toBeVisible();
    await expect(page.locator('legend:has-text("Views Range")')).toBeVisible();
    await expect(page.locator('legend:has-text("Date Range")')).toBeVisible();
  });

  test("domain checkboxes toggle active dot on Filter button", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Open filters"]').click();
    await page.waitForTimeout(300);

    const domainCheckboxes = page.locator('fieldset:has(legend:has-text("Domain")) input[type="checkbox"]');
    await domainCheckboxes.first().check();
    await page.waitForTimeout(300);

    const filterBtn = page.locator('button[aria-label="Open filters"]');
    await expect(filterBtn.locator(".rounded-full")).toBeVisible();

    await domainCheckboxes.first().uncheck();
    await page.waitForTimeout(300);
    await expect(filterBtn.locator(".rounded-full")).not.toBeVisible();
  });

  test("likes range inputs accept values", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('button[aria-label="Open filters"]').click();
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

    await page.locator('button[aria-label="Open filters"]').click();
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

    await page.locator('button[aria-label="Open filters"]').click();
    await page.waitForTimeout(300);

    await page.locator('fieldset:has(legend:has-text("Domain")) input[type="checkbox"]').first().check();
    await page.locator('input[aria-label="Min likes"]').fill("5000");
    await page.waitForTimeout(300);

    await page.getByText("Clear All").click();
    await page.waitForTimeout(300);

    const checked = await page.locator('input[type="checkbox"]:checked').count();
    expect(checked).toBe(0);
    await expect(page.locator('input[aria-label="Min likes"]')).toHaveValue("");
  });
});

test.describe("Posts page — combined search and filters", () => {
  test("keyword search works alongside filters", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    const searchInput = page.locator('input[aria-label="Search posts"]');
    await searchInput.fill("deploy");
    await page.waitForTimeout(1000);

    await expect(page.locator(".ag-root")).toBeVisible();
  });

  test("filter and search compose", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });

    await page.locator('input[aria-label="Search posts"]').fill("ai");
    await page.waitForTimeout(1000);

    await page.locator('button[aria-label="Open filters"]').click();
    await page.waitForTimeout(300);
    await page.locator('fieldset:has(legend:has-text("Domain")) input[type="checkbox"]').first().check();
    await page.waitForTimeout(300);

    await expect(page.locator(".ag-root")).toBeVisible();
    await expect(page.locator('button[aria-label="Open filters"] .rounded-full')).toBeVisible();
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

test.describe("Platform icons", () => {
  test("posts grid renders platform icons", async ({ page }) => {
    await page.goto(`${BASE}/posts`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    await expect(page.locator('svg[aria-label="Instagram"]').first()).toBeVisible({ timeout: 8000 });
  });

  test("overview hot posts render platform icons", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=HOT POSTS", { timeout: 10000 });
    await expect(page.locator('svg[aria-label="Instagram"]').first()).toBeVisible({ timeout: 8000 });
  });

  test("signals table renders platform icons", async ({ page }) => {
    await page.goto(`${BASE}/signals`);
    await page.waitForSelector("text=High-Signal Posts", { timeout: 10000 });
    await expect(page.locator('svg[aria-label="Instagram"]').first()).toBeVisible({ timeout: 8000 });
  });
});

test.describe("Hot posts — post vs creator click targets", () => {
  test("hot post cards link to the source post", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=HOT POSTS", { timeout: 10000 });
    await expect(page.locator('a[href*="instagram.com/p/"]').first()).toBeVisible();
  });

  test("creator navigation links exist separately from post links", async ({ page }) => {
    await page.goto(BASE);
    await page.waitForSelector("text=HOT POSTS", { timeout: 10000 });
    await expect(page.locator('a[href*="/creators/"]').first()).toBeVisible({ timeout: 8000 });
  });
});

test.describe("Signals page — appearance", () => {
  test("profile column shows avatars", async ({ page }) => {
    await page.goto(`${BASE}/signals`);
    await page.waitForSelector("text=High-Signal Posts", { timeout: 10000 });
    await expect(page.locator("img[alt]").first()).toBeVisible({ timeout: 8000 });
  });

  test("columns sort on header click", async ({ page }) => {
    await page.goto(`${BASE}/signals`);
    await page.waitForSelector("text=High-Signal Posts", { timeout: 10000 });
    await page.locator('button[aria-label="Sort by RANK"]').click();
    await expect(page.locator('th[aria-sort="ascending"]')).toBeVisible();
  });
});

test.describe("Creators page", () => {
  test("creators list loads sortable columns", async ({ page }) => {
    await page.goto(`${BASE}/creators`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    const headers = page.locator(".ag-header-cell-text");
    await expect(headers.getByText("Creator", { exact: true })).toBeVisible();
    await expect(headers.getByText("Platforms", { exact: true })).toBeVisible();
    await expect(headers.getByText("Posts", { exact: true })).toBeVisible();
    await expect(headers.getByText("Standouts", { exact: true })).toBeVisible();
    await expect(headers.getByText("Hot", { exact: true })).toBeVisible();
  });

  test("creator filter modal opens with a text search bar", async ({ page }) => {
    await page.goto(`${BASE}/creators`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    await page.locator('button[aria-label="Open filters"]').click();
    await expect(page.locator('input[aria-label="Search creators"]')).toBeVisible();
  });

  test("creators table has no per-column filter chevrons", async ({ page }) => {
    await page.goto(`${BASE}/creators`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    await expect(page.locator(".ag-header-cell .ag-icon-filter:visible")).toHaveCount(0);
  });

  test("creators list renders platform icon collection", async ({ page }) => {
    await page.goto(`${BASE}/creators`);
    await page.waitForSelector(".ag-root", { timeout: 15000 });
    const platformIcons = page.locator(".ag-cell svg");
    await expect(platformIcons.first()).toBeVisible({ timeout: 8000 });
  });
});
