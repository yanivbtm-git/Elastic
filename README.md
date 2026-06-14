# AI Job Search Engine (GitHub Pages)

Automated AI-assisted job search pipeline and tracker with:

- Claude Opus web job discovery + ATS scraping
- Claude Haiku fit scoring
- strict source-of-truth merge behavior for `scored_jobs.json`
- GitHub Actions automation (scheduled refresh + Pages deploy)
- Material 3, mobile-first tracker PWA in `docs/index.html`

## Repository layout

- `run_pipeline.py` - end-to-end search, scrape, score, filter, save, build flow
- `config.json` - candidate profile, preferences, pipeline settings
- `profile.md` - candidate profile context passed into AI prompts
- `scored_jobs.json` - **single source of truth** for tracked jobs
- `templates/index.template.html` - HTML template used to build the tracker
- `docs/index.html` - built tracker deployed to GitHub Pages
- `.github/workflows/jobs-pipeline.yml` - scheduled refresh (Sun-Thu, 3x daily)
- `.github/workflows/pages-deploy.yml` - deploys `docs/` to GitHub Pages

## Critical data rule

`scored_jobs.json` is the only source of truth.

If a job has `initial_status`, it is treated as a manual entry and **never deleted**
by pipeline refreshes. Merge behavior is:

`pipeline results + preserved manual entries`.

## Local run

```bash
python3 run_pipeline.py
```

Optional auto-commit/push (for CI use):

```bash
python3 run_pipeline.py --auto-push
```

## Required secrets for AI mode

Set in GitHub repository secrets:

- `ANTHROPIC_API_KEY`
- optional model overrides:
  - `ANTHROPIC_OPUS_MODEL`
  - `ANTHROPIC_HAIKU_MODEL`

Without API key, pipeline still runs with heuristic search parsing and fallback scoring.
