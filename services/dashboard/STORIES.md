# Dashboard User Stories

## US-1: Caption text does not overflow into adjacent columns

**As a** data analyst viewing the posts table
**I want** long captions to be truncated with an ellipsis within their column
**So that** I can scan rows without text bleeding into Likes, Comments, or Views columns.

### Acceptance criteria

- [ ] Captions longer than the column width show `…` at the cut point
- [ ] No horizontal scrollbar appears on the caption column alone
- [ ] Resizing the caption column wider reveals more text
- [ ] Empty captions display "no caption" in muted italic
- [ ] Caption text is selectable for copy (enableCellTextSelection works)
- [ ] Number columns (Likes, Comments, Views) display only their formatted values — no caption spill

### Technical notes

Root cause: `.ag-cell { display: flex }` prevented `text-overflow: ellipsis` from working.
Fix: `!block` class on caption cells with `overflow: hidden; text-overflow: ellipsis; white-space: nowrap`.

---

## US-2: Full-text search across posts

**As a** content researcher
**I want** to type keywords into a search bar above the posts grid
**So that** I can find posts matching my search terms across captions, topics, and usernames.

### Acceptance criteria

- [ ] Search bar is visible above the AG Grid on the Posts page
- [ ] Typing 2+ characters triggers a debounced (300ms) server-side search
- [ ] Results update in the grid within 2 seconds for typical queries
- [ ] Search matches across: caption text, owner_username, gold_topic, gold_domain
- [ ] Search is case-insensitive (DuckDB ILIKE)
- [ ] Clearing the search bar resets to the full dataset
- [ ] Search bar has a clear (X) button visible when text is present
- [ ] Empty search results display "0 posts" gracefully (not an error)
- [ ] Typing fewer than 2 characters does not trigger a search

### Technical notes

Backend: `GET /api/search?q=<term>&limit=500` queries `v_post_detail` with ILIKE.
Frontend: debounced fetch via `fetchSearchResults()`, results replace grid data.

---

## US-3: Domain filter chips (SUPERSEDED by US-7 Advanced Panel)

Replaced by the Advanced Filter panel which offers multi-select domain checkboxes
alongside rank, likes range, and date range filters in a unified interface.

---

## US-4: Admiralty rank filter chips (SUPERSEDED by US-7 Advanced Panel)

Replaced by the Advanced Filter panel which offers multi-select rank tier checkboxes
alongside domain, likes range, and date range filters in a unified interface.

---

## US-5: Combined search and filters

**As a** power user
**I want** to combine keyword search with per-column filters and the advanced filter panel
**So that** I can execute precise queries like "deployment posts from Tech creators ranked A with >10K likes".

### Acceptance criteria

- [ ] Keyword search, column filters, and advanced panel filters all compose (AND logic)
- [ ] Changing any filter preserves other active filters
- [ ] "X posts · advanced filters active" indicator shows when panel filters are set
- [ ] Column filter icons show active state (highlighted) when that column has a filter applied
- [ ] All filters can be cleared independently

### Technical notes

Server-side: keyword search (`/api/search` with ILIKE).
Client-side: AG Grid column filters (text/number/date/boolean) + external filter (advanced panel).
All three layers compose: row data → search results → column filter → external filter.

---

## US-6: Per-column filter popups with type-specific UI

**As a** data analyst
**I want** to click a filter icon in each column header to open a type-appropriate filter popup
**So that** I can filter by text, number, date, or boolean without clutter in the header row.

### Acceptance criteria

- [ ] Every column header has a filter icon (funnel) — no inline floating filter inputs
- [ ] Clicking the filter icon opens a popup with filter options matching the column type
- [ ] Text columns show: Contains, Not contains, Equals, Starts with + text input
- [ ] Numeric columns show: Equals, Greater than, Less than, In range + number inputs
- [ ] Date column shows: Equals, Before, After, In range + date picker
- [ ] Boolean columns (Edu, Act) show custom radio: All / Yes / No
- [ ] Filter popup uses dark theme (matches `--ag-*` CSS variables)
- [ ] Active filter icon is highlighted (filled funnel)
- [ ] Clicking column header text sorts (ascending/descending toggle)
- [ ] Sort direction is indicated by arrow in the header

### Technical notes

AG Grid Community built-in filters with `filter: true` (no `floatingFilter`).
Industry-standard UX: Excel, Google Sheets, Airtable all use filter icons in headers
with popup-based filtering — never inline inputs below every column.

## US-7: Advanced filter panel

**As a** content strategist
**I want** to open an advanced filter panel with multi-select checkboxes and range inputs
**So that** I can apply multiple cross-column filters simultaneously without clicking each column header.

### Acceptance criteria

- [ ] "Advanced" button is visible next to the search bar
- [ ] Clicking the button toggles a filter panel below the search bar
- [ ] Panel shows four filter groups: Domain (checkboxes), Rank Tier (checkboxes), Likes Range (min/max), Date Range (from/to)
- [ ] Checkbox selections toggle on click
- [ ] Range inputs accept numeric values
- [ ] Date inputs use native date picker with dark color scheme
- [ ] Active dot indicator appears on the Advanced button when any filter is set
- [ ] "Clear All" button resets all advanced filters
- [ ] Panel can be collapsed by clicking the Advanced button again
- [ ] Panel has `aria-expanded` attribute for screen reader state

---

## US-8: Boolean column filters (Edu / Act)

**As a** content reviewer
**I want** to filter the Edu and Act columns via a simple Yes/No/All radio selector
**So that** I can quickly isolate educational or actionable posts.

### Acceptance criteria

- [ ] Clicking the filter icon on Edu or Act column opens a 3-option radio: All, Yes, No
- [ ] Selecting "Yes" shows only rows where the column value is true
- [ ] Selecting "No" shows only rows where the column value is false or null
- [ ] Selecting "All" clears the filter
- [ ] Filter popup uses the dark theme matching the grid
- [ ] Filter icon is highlighted when a non-"All" selection is active
- [ ] Filter state persists across grid interactions (sort, pagination)

### Technical notes

AG Grid Community does not ship a set filter. Custom `BooleanFilter` class implements
`IFilterComp` interface with radio button UI rendered as raw DOM for performance.
