# Dashboard Screenshot — Provenance Note

## What the asset shows

`presentation/dashboard_screenshot.png` is a desktop capture of the **Streamlit executive
dashboard** (`src/dashboard.py`) on its default **Executive Overview** view. It shows the
corpus switcher and navigation in the sidebar, the four pipeline KPI cards, the topic-volume
bar chart, topic-performance table, the interactive VADER sentiment box plot, and the
collapsed publication-quality static-artifact expander. It is the visual evidence used in
the presentation deck and the visualization/UI rubric section of `PROJECT_REPORT.md`.

## Capture facts

- **File:** `presentation/dashboard_screenshot.png`
- **Canvas:** approximately **1600 × 1050 px** (verified from the PNG header)
- **Capture date:** the image was captured during the project's dashboard-verification phase in
  **November 2026** alongside the first end-to-end Streamlit smoke run. An exact timestamp is
  not embedded in the PNG; check `git log -- presentation/dashboard_screenshot.png` for the
  committed version date.

## Exact regeneration steps

### Option A — live Streamlit app (preferred)

1. From the project root, run `streamlit run src/dashboard.py`.
2. Open the URL Streamlit prints (normally `http://localhost:8501`).
3. Set the browser viewport to **1600 × 1050** (e.g. Chrome DevTools → device toolbar →
   custom viewport, or maximize a 1600 × 1050 display).
4. Select **Synthetic Benchmark (6,000 articles)** and the **Executive Overview** view; wait
   for all charts to finish rendering.
5. Capture a **viewport screenshot** at 1600 × 1050 and save it as
   `presentation/dashboard_screenshot.png`.

### Option B — offline HTML dashboard (no server)

1. Ensure the artifact exists: `python src/build_interactive_dashboard.py`.
2. Open `dashboard/index.html` and choose **interactive_dashboard.html** (synthetic) or
   **interactive_dashboard_real.html** (Reuters).
3. Set the viewport to 1600 × 1050, capture the header, KPI cards and decision panel, and
   save the capture as `presentation/dashboard_screenshot.png`.

> Keep the capture at 1600 × 1050 so the presentation slide layout and this provenance note
> stay consistent. If the UI changes, re-run the capture rather than reusing a stale image.
