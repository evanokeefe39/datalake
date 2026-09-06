"""Generate a self-contained HTML report on faceless listicle/carousel account
performance from the datalake.

Reads the consolidated analysis snapshot (report_data.json in analysis/output/)
and writes listicle_report.html beside this script. Charting (Chart.js) and
styling (Tailwind) load from CDN, so the HTML needs an internet connection to
render fully but is otherwise portable.

Run:  uv run python analysis/listicle_report_gen.py
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "output" / "report_data.json"
OUT = HERE / "listicle_report.html"


def main() -> None:
    with open(DATA, encoding="utf-8") as fh:
        data = json.load(fh)

    # ---- Time series: align months where both cohorts have judged posts ----
    cidx = {d["m"]: i for i, d in enumerate(data["time"]["carousel_list"]) if d["n"] > 0}
    oidx = {d["m"]: i for i, d in enumerate(data["time"]["other_faceless"]) if d["n"] > 0}
    common = [m for m in sorted(cidx) if m in oidx]
    car_al = [data["time"]["carousel_list"][cidx[m]]["sr"] for m in common]
    oth_al = [data["time"]["other_faceless"][oidx[m]]["sr"] for m in common]
    medL_car = [data["time"]["carousel_list"][cidx[m]]["medL"] for m in common]
    medL_oth = [data["time"]["other_faceless"][oidx[m]]["medL"] for m in common]

    # ---- Domain delta + colors ----
    dom = data["domain"]
    dom_labels = [d["domain"] for d in dom]
    dom_delta = [d["delta"] for d in dom]
    def dom_col(d: float) -> str:
        if d >= 5:
            return "rgba(16,185,129,.85)"
        if d <= -4:
            return "rgba(244,63,94,.85)"
        return "rgba(148,163,184,.85)"
    dom_colors = [dom_col(d) for d in dom_delta]

    # ---- Format-level ----
    fmt = data["post_format"]
    fmt_labels = ["Carousel / listicle", "Single image + text", "Talking head"]
    fmt_st = [fmt["carousel"]["standout_rate"], fmt["single_text_image"]["standout_rate"], fmt["talking_head"]["standout_rate"]]
    fmt_hot = [fmt["carousel"]["hot_rate"], fmt["single_text_image"]["hot_rate"], fmt["talking_head"]["hot_rate"]]
    fmt_medL = [fmt["carousel"]["med_likes"], fmt["single_text_image"]["med_likes"], fmt["talking_head"]["med_likes"]]
    fmt_n = [fmt["carousel"]["n"], fmt["single_text_image"]["n"], fmt["talking_head"]["n"]]

    # ---- Sub-niche wins / losses (filter tiny samples) ----
    niche = sorted([x for x in data["niche"] if x["carN"] >= 8], key=lambda x: -x["delta"])

    total_posts = (data["cohort_counts"]["other_faceless"]
                   + data["cohort_counts"]["carousel_list"]
                   + data["cohort_counts"]["talking_head"])

    # ---- Account callout table rows ----
    callout_html_rows = []
    for _o, v in sorted(data["callouts"].items(), key=lambda kv: -(kv[1]["sr"] or 0)):
        c = v["car"]
        med_l = v["medL"] if v["medL"] and v["medL"] > 0 else "&mdash;"
        callout_html_rows.append(
            f"<tr class='border-b border-slate-100'>"
            f"<td class='py-2 px-3 font-medium text-slate-800'>{v['owner']}</td>"
            f"<td class='py-2 px-3 text-slate-600'>{v['niche']}</td>"
            f"<td class='py-2 px-3 text-right'>{c}%</td>"
            f"<td class='py-2 px-3 text-right font-semibold'>{v['sr']}%</td>"
            f"<td class='py-2 px-3 text-right'>{v['recent']}%</td>"
            f"<td class='py-2 px-3 text-right'>{med_l}</td></tr>"
        )
    callout_rows = "\n".join(callout_html_rows)

    win_rows = "\n".join(
        f"<tr class='border-b border-slate-100'>"
        f"<td class='py-1.5 px-2 font-medium'>{x['niche']}</td>"
        f"<td class='py-1.5 px-2 text-right'>{x['carN']}</td>"
        f"<td class='py-1.5 px-2 text-right text-emerald-600 font-semibold'>+{x['delta']:.1f}pp</td></tr>"
        for x in niche if x["delta"] >= 4)
    loss_rows = "\n".join(
        f"<tr class='border-b border-slate-100'>"
        f"<td class='py-1.5 px-2 font-medium'>{x['niche']}</td>"
        f"<td class='py-1.5 px-2 text-right'>{x['carN']}</td>"
        f"<td class='py-1.5 px-2 text-right text-rose-600 font-semibold'>{x['delta']:.1f}pp</td></tr>"
        for x in sorted([x for x in niche if x["delta"] <= -4], key=lambda x: x["delta"]))

    wa = data["within_account"]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Faceless Listicle / Carousel Accounts &mdash; Performance Analysis</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
</head>
<body class="bg-slate-50 text-slate-800">
<div class="max-w-6xl mx-auto px-6 py-10">

  <header class="mb-10">
    <p class="text-sm font-medium text-indigo-600 mb-2">datalake &middot; content strategy analysis</p>
    <h1 class="text-4xl font-extrabold text-slate-900 leading-tight">How faceless listicle &amp; carousel accounts perform on Instagram</h1>
    <p class="mt-3 text-lg text-slate-600 max-w-3xl">A within-corpus comparison over <strong>{total_posts:,}</strong> enriched posts across
       <strong>95</strong> accounts, using baseline-normalized standout/hot labels so results are fair across accounts of different sizes.</p>
    <div class="mt-5 flex flex-wrap gap-2 text-sm">
      <span class="inline-flex items-center rounded-full bg-slate-200 px-2.5 py-0.5 text-xs font-semibold text-slate-700">Data window: Oct 2025 &ndash; Aug 2026</span>
      <span class="inline-flex items-center rounded-full bg-slate-200 px-2.5 py-0.5 text-xs font-semibold text-slate-700">Metric: standout% (per-post, vs own trailing baseline)</span>
    </div>
  </header>

  <div class="bg-amber-50 border border-amber-200 text-amber-900 rounded-2xl p-5 mb-10 text-sm leading-relaxed">
    <strong>Read this first.</strong> This lake is a <em>curated</em> scrape &mdash; Tech + Business &asymp; 67% of posts, ~79% educational &mdash; not a random
    sample of Instagram. Results describe how these strategies perform <em>within this cohort</em>, and &ldquo;listicle&rdquo; is inferred from
    carousel/slideshow format plus low talking-head share (the pipeline has no explicit listicle or face label). Sub-niche deltas on small samples
    (n &lt; 30) are indicative, not conclusive.
  </div>

  <section class="mb-10">
    <h2 class="text-2xl font-bold text-slate-900 mb-1">Headline</h2>
    <p class="text-slate-600 mb-6 max-w-3xl">Carousels are a <em>volume + reach</em> play, not an engagement-rate winner. On the metric that corrects
      for account size &mdash; share of posts clearing each account's own baseline (standout) &mdash; the 12 pure carousel-listicle accounts sit
      <strong>below</strong> the other-faceless accounts, and their edge over their own accounts' single-image posts is small. Their raw-likes
      advantage mostly reflects larger audiences.</p>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{fmt_st[0]:.1f}%</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">carousel standout rate</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{fmt_st[1]:.1f}%</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">single-image standout rate</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{fmt_st[2]:.1f}%</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">talking-head standout rate</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-indigo-600">+{wa['mean_diff']:.1f}pp</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">avg carousel edge within same account</div></div>
    </div>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{medL_car[-1]:,}</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">carousel median likes (Aug 26)</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{medL_oth[-1]:,}</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">other-faceless median likes (Aug 26)</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{data['cohort_counts']['carousel_list']:,}</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">posts by 12 pure carousel-list accounts</div></div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-5">
        <div class="text-3xl font-bold text-slate-900">{wa['win']} / {wa['lose']}</div>
        <div class="text-xs font-medium uppercase tracking-wide text-slate-500 mt-1">accounts where carousels won/lost vs own mix</div></div>
    </div>
  </section>

  <section class="mb-12">
    <h2 class="text-2xl font-bold text-slate-900 mb-4">1 &middot; Which format performs best, post for post?</h2>
    <div class="grid md:grid-cols-2 gap-6">
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
        <canvas id="fmtChart" height="260"></canvas>
        <p class="text-xs text-slate-500 mt-3">Standout rate (% of posts clearing each account's own baseline). N: carousel {fmt_n[0]}, single {fmt_n[1]}, talking-head {fmt_n[2]}.</p>
      </div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
        <canvas id="medLikesChart" height="260"></canvas>
        <p class="text-xs text-slate-500 mt-3">Median raw likes per post. Carousels lead here &mdash; but that is follower/audience size, not format quality.</p>
      </div>
    </div>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 mt-6">
      <h3 class="font-semibold text-slate-900 mb-2">What this means</h3>
      <ul class="list-disc pl-5 text-sm text-slate-700 space-y-1.5">
        <li><strong>Talking-head posts outperform on engagement quality</strong> &mdash; {fmt_st[2]}% standout, the highest, though only 3 pure talking-head accounts feed it (small sample).</li>
        <li>Carousels and single-image text are near-tied on standout ({fmt_st[0]}% vs {fmt_st[1]}%); carousels win on raw reach (median {fmt_medL[0]:,.0f} vs {fmt_medL[1]:,.0f} likes).</li>
        <li><strong>Within an account</strong> (same audience, same creator), carousels beat that account's other formats on only ~{wa['win'] / wa['accts'] * 100:.0f}% of accounts ({wa['win']} of {wa['accts']}), and the average edge is just <strong>+{wa['mean_diff']:.1f}pp</strong> &mdash; driven by a handful of large winners.</li>
      </ul>
    </div>
  </section>

  <section class="mb-12">
    <h2 class="text-2xl font-bold text-slate-900 mb-4">2 &middot; How engagement trends over time</h2>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
      <canvas id="timeChart" height="300"></canvas>
      <div class="mt-2 flex flex-wrap gap-4 text-sm">
        <span class="inline-flex items-center gap-1.5"><span class="w-3 h-3 rounded-full bg-indigo-500 inline-block"></span>Pure carousel-list accounts</span>
        <span class="inline-flex items-center gap-1.5"><span class="w-3 h-3 rounded-full bg-emerald-500 inline-block"></span>Other faceless accounts</span>
      </div>
      <p class="text-xs text-slate-500 mt-3">Monthly standout rate (%). Sparse early months for the small carousel cohort; months shown have &ge;1 judged post for both groups.</p>
    </div>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 mt-6">
      <h3 class="font-semibold text-slate-900 mb-2">What this means</h3>
      <ul class="list-disc pl-5 text-sm text-slate-700 space-y-1.5">
        <li>Both strategies <strong>saturate</strong>: standout rates drift down across 2026 as posting volume climbs and baselines tighten &mdash; a whole-corpus effect, not format-specific.</li>
        <li>The carousel-listicle cohort has tracked <em>at or below</em> the other-faceless cohort for most of the window (Apr&ndash;May dip to ~4&ndash;7% standout), recovering to ~{car_al[-1]:.0f}% by Aug.</li>
        <li>Raw median likes for carousel accounts also fell ({medL_car[0]:,} &rarr; {medL_car[-1]:,}) as more accounts entered the space &mdash; a classic supply-side squeeze on faceless content.</li>
      </ul>
    </div>
  </section>

  <section class="mb-12">
    <h2 class="text-2xl font-bold text-slate-900 mb-4">3 &middot; Where the carousel/listicle format wins and loses</h2>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 mb-6">
      <h3 class="font-semibold text-slate-900 mb-3">By top-level domain &mdash; carousel standout minus non-carousel standout (pp)</h3>
      <canvas id="domChart" height="240"></canvas>
    </div>
    <div class="grid md:grid-cols-2 gap-6">
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
        <h3 class="font-semibold text-emerald-700 mb-3">Wins hard here</h3>
        <div class="overflow-x-auto"><table class="w-full text-sm">
          <thead><tr class="text-left text-slate-500 text-xs uppercase"><th class="py-1 px-2">Sub-niche</th><th class="py-1 px-2 text-right">car N</th><th class="py-1 px-2 text-right">carousel edge</th></tr></thead>
          <tbody>{win_rows}</tbody></table></div>
      </div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
        <h3 class="font-semibold text-rose-700 mb-3">Underperforms here</h3>
        <div class="overflow-x-auto"><table class="w-full text-sm">
          <thead><tr class="text-left text-slate-500 text-xs uppercase"><th class="py-1 px-2">Sub-niche</th><th class="py-1 px-2 text-right">car N</th><th class="py-1 px-2 text-right">carousel edge</th></tr></thead>
          <tbody>{loss_rows}</tbody></table></div>
      </div>
    </div>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 mt-6 text-sm text-slate-700 leading-relaxed">
      <strong>The pattern:</strong> the carousel/listicle format over-indexes in <em>aspirational, list-friendly, self-improvement</em> niches &mdash;
      freelancing, social-media marketing, personal finance, sales, AI-art how-tos. It <em>underperforms</em> in <em>skills/credentials-demanding</em>
      niches &mdash; software engineering, cybersecurity, education, digital marketing. Swipeable listicles win where viewers want quick tips; they lose
      where audiences expect depth and proof.
      <span class="text-amber-700 block mt-2">Caveat: several &ldquo;winning&rdquo; niches are dominated by a single star account (e.g. Sales &asymp; jackblairofficial
      alone), so the delta partly reflects creator skill, not just the format.</span>
    </div>
  </section>

  <section class="mb-12">
    <h2 class="text-2xl font-bold text-slate-900 mb-4">4 &middot; The pure carousel-listicle accounts, ranked</h2>
    <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
      <div class="overflow-x-auto"><table class="w-full text-sm">
        <thead><tr class="text-left text-slate-500 text-xs uppercase border-b border-slate-200">
          <th class="py-2 px-3">Account</th><th class="py-2 px-3">Dominant niche</th><th class="py-2 px-3 text-right">% carousel</th>
          <th class="py-2 px-3 text-right">standout%</th><th class="py-2 px-3 text-right">recent (Jun&ndash;Aug)</th>
          <th class="py-2 px-3 text-right">median likes</th></tr></thead>
        <tbody>{callout_rows}</tbody>
      </table></div>
      <p class="text-xs text-slate-500 mt-3">Accounts with &ge;50% carousel posts and &lt;25% talking-head. &ldquo;recent&rdquo; = standout rate Jun&ndash;Aug 2026; &ldquo;&mdash;&rdquo; = too few posts.</p>
    </div>
    <div class="grid md:grid-cols-2 gap-6 mt-6">
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 text-sm text-slate-700 leading-relaxed">
        <h3 class="font-semibold text-slate-900 mb-2">What separates the winners</h3>
        <p>The strongest carousel-list accounts (jackblairofficial, girsta) sit on the higher end but still trail the corpus's best faceless all-rounders
        (tembrasdev 34.5%, mrnotion.co 29%). Notably, the highest <em>raw-likes</em> carousel accounts are <em>not</em> the highest-quality &mdash;
        reach &ne; standout. The format works, but it does not create unfair advantage by itself.</p>
      </div>
      <div class="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 text-sm text-slate-700 leading-relaxed">
        <h3 class="font-semibold text-slate-900 mb-2">Bottom line for the strategy</h3>
        <p>Faceless listicle/carousel accounts are a viable <em>volume and reach</em> model in this cohort, best applied to self-improvement and
        quick-tip niches (finance, freelancing, marketing, sales). They saturate fast and face a supply squeeze. For depth/credibility niches the
        format is a <em>drag</em> &mdash; talking-head or single-image with real substance outperforms. The ceiling is set by the creator, not the carousel.</p>
      </div>
    </div>
  </section>

  <footer class="text-xs text-slate-400 border-t border-slate-200 pt-6">
    Generated from datalake (data/state.duckdb) &middot; {total_posts:,} enriched Instagram posts &middot;
    Method caveats: curated cohort (not market-wide), inferred listicle definition, no follower-time-series normalization (label system used instead).
  </footer>

</div>

<script>
Chart.defaults.font.family = "ui-sans-serif, system-ui, sans-serif";
Chart.defaults.color = "#475569";

new Chart(document.getElementById('fmtChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(fmt_labels)},
    datasets: [
      {{ label: 'Standout %', data: {json.dumps(fmt_st)}, backgroundColor: 'rgba(99,102,241,.85)', borderRadius: 6 }},
      {{ label: 'Hot %', data: {json.dumps(fmt_hot)}, backgroundColor: 'rgba(244,63,94,.75)', borderRadius: 6 }}
    ]
  }},
  options: {{ plugins: {{ legend: {{ position: 'bottom' }} }}, scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: '% of posts' }} }} }} }}
}});

new Chart(document.getElementById('medLikesChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(fmt_labels)},
    datasets: [{{ label: 'Median likes', data: {json.dumps(fmt_medL)}, backgroundColor: 'rgba(14,165,233,.85)', borderRadius: 6 }}]
  }},
  options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'median likes' }} }} }} }}
}});

new Chart(document.getElementById('timeChart'), {{
  type: 'line',
  data: {{
    labels: {json.dumps(common)},
    datasets: [
      {{ label: 'Pure carousel-list', data: {json.dumps(car_al)}, borderColor: '#6366f1', backgroundColor: 'rgba(99,102,241,.1)', fill: true, tension: .25, pointRadius: 3 }},
      {{ label: 'Other faceless', data: {json.dumps(oth_al)}, borderColor: '#10b981', backgroundColor: 'rgba(16,185,129,.08)', fill: true, tension: .25, pointRadius: 3 }}
    ]
  }},
  options: {{ plugins: {{ legend: {{ position: 'bottom' }} }}, scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'standout %' }} }} }} }}
}});

new Chart(document.getElementById('domChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(dom_labels)},
    datasets: [{{ label: 'Carousel &minus; non-carousel standout (pp)', data: {json.dumps(dom_delta)},
      backgroundColor: {json.dumps(dom_colors)}, borderRadius: 6 }}]
  }},
  options: {{ indexAxis: 'y', plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ title: {{ display: true, text: 'delta pp' }} }} }} }}
}});
</script>
</body>
</html>"""
    OUT.write_text(html, encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
