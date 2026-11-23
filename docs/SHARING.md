# Sharing the Project

This project ships **three static dashboards** that need no server, database or API
key at view time. That makes public sharing a one-time setup rather than a hosting
project.

| Artifact | Size | Needs a server? | Best for |
|---|---:|:---:|---|
| `dashboard/command_center.html` | ~2 MB | No | The main public link: dark-mode topic/entity command center |
| `dashboard/interactive_dashboard.html` | ~5 MB | No | Synthetic-corpus Plotly export (seven figures + evidence panel) |
| `dashboard/interactive_dashboard_real.html` | ~5 MB | No | Reuters-corpus Plotly export |
| `src/dashboard.py` | — | Yes (Streamlit) | Live filtering, semantic treemaps, corpus switcher |

---

## 1. GitHub Pages (recommended, already automated)

The workflow `.github/workflows/pages.yml` uploads the whole `dashboard/`
directory to GitHub Pages on every push to `main`.

1. Open **Settings → Pages**.
2. Under **Build and deployment → Source**, choose **GitHub Actions**.
   The workflow also attempts to enable this automatically on its first run.
3. Push to `main` (or run **Actions → Publish Dashboards → Run workflow**).
4. The public URL is:

   ```
   https://omarhanii-bit.github.io/News-Article-Analysis-and-Topic-Extraction/
   ```

The landing page links to all three dashboards. Everything is relative-path, so
the site also works from a project subpath, a USB stick, or an email attachment.

**Why this is enough for “share with everyone”:** the Command Center embeds its
data in the HTML file. A reviewer opens the link, and the topic map, timeline,
entity bars, feed, filters and article drawer all work without a backend.

### If Pages is not enabled automatically

- Confirm the repository is public (Pages on private repos needs a paid plan).
- Set **Settings → Pages → Source: GitHub Actions** manually, then re-run the
  workflow.
- The first deployment can take 1–2 minutes. The workflow summary prints the
  exact URL.

---

## 2. Streamlit Community Cloud (optional live app)

Use this when you want the *interactive* Streamlit app rather than the static
exports.

1. Sign in at <https://share.streamlit.io> with GitHub.
2. **New app** → repository
   `OmarHanii-bit/News-Article-Analysis-and-Topic-Extraction` → branch `main`.
3. Main file path: `src/dashboard.py`.
4. Python version: **3.11**.
5. Under **Advanced settings → Requirements file**, use
   `requirements-dashboard.txt` (a slim file that avoids installing PySpark,
   Jupyter and the presentation toolchain on the free tier).
6. Deploy.

Notes:

- The analytics tabs read the committed JSON summaries, so they work from a
  fresh clone.
- The **article search** tab reads the Parquet lakehouse
  (`data/processed_parquet`, `data/real_news/processed_parquet`), which is
  git-ignored as regenerable. Run `python run_all.py --force` locally before
  deploying if you need that tab, or commit the two small Parquet directories.
- Free-tier apps sleep after inactivity; the first request wakes them in a few
  seconds.

---

## 3. Docker / Render / Railway (self-hosted Streamlit)

The repository already contains a multi-stage `Dockerfile` (Python 3.11 +
OpenJDK 17 + Streamlit on port 8501):

```bash
docker build -t news-intelligence .
docker run --rm -p 8501:8501 news-intelligence
# open http://localhost:8501
```

For Render or Railway, create a **Web Service** from the repository, choose
**Docker**, and expose port `8501`. The container’s health check is
`/_stcore/health`.

A free 512 MB instance may be tight because the image includes PySpark; the
static GitHub Pages dashboards avoid that cost entirely.

---

## 4. Share on a local network (no cloud account)

```bash
streamlit run src/dashboard.py --server.address 0.0.0.0 --server.port 8501
```

Then share `http://<your-LAN-IP>:8501` with devices on the same network. Allow
port 8501 through the firewall if prompted. This is useful for a classroom or
demo table, but it is not a public URL.

---

## 5. Share a single file

`dashboard/command_center.html` is self-contained. You can email it, put it on a
USB stick, or attach it to a submission. The only network request is the
optional Google Fonts stylesheet; offline it falls back to the system sans-serif
and monospace fonts, and every chart still renders.

To regenerate it after changing the data:

```bash
python src/build_command_center.py
```

To regenerate the Plotly exports:

```bash
python src/build_interactive_dashboard.py
```

---

## What is safe to publish

- The repository contains **no credentials**. `MONGODB_URI`, `KAGGLE_USERNAME`
  and similar values live in `.env` (git-ignored); `.env.example` documents the
  names only.
- The synthetic corpus is team-generated. The Reuters-21578 corpus is a public
  NLTK archive; see `data/real_news/provenance.json` for source and licence.
- The dashboards do not call external APIs or load remote JavaScript. The
  Command Center embeds its JSON payload and uses only inline CSS/JS.

## Verification checklist

- [ ] `python tests/run_tests.py` passes locally.
- [ ] `python src/build_command_center.py` rewrites the dashboard without error.
- [ ] The Pages workflow is green under **Actions**.
- [ ] The public URL opens the landing page and the Command Center.
- [ ] Search, source filter, topic click, timeline brush and article drawer all
      respond in the public page (not just locally).
