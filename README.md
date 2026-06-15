# AI Job Search Engine (GitHub Pages)

Automated AI-assisted job search pipeline and tracker with:

- Gemini web job discovery + ATS scraping
- Gemini fit scoring
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

Pipeline filtering includes:
- minimum fit score threshold
- location match requirement
- posted-within-30-days requirement
- role-title match against configured target roles

Rate-limit resilience:
- Gemini calls retry on transient API errors (429/503/etc.) with backoff.
- Scoring candidates are capped per run via `pipeline.scoring.max_ai_candidates_per_run`.
- AI scoring auto-disables for the remainder of a run after too many consecutive model errors.

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

- `GEMINI_API_KEY`
- optional model overrides:
  - `GEMINI_PRO_MODEL`
  - `GEMINI_FLASH_MODEL`
  - recommended values:
    - `GEMINI_PRO_MODEL=gemini-2.5-pro`
    - `GEMINI_FLASH_MODEL=gemini-2.5-flash`

Without API key, pipeline still runs with heuristic search parsing and fallback scoring.

## Privacy mode

Privacy defaults are enabled in `config.json`:

- `privacy.redact_personal_data: true`
- `privacy.pipeline_profile_mode: "summary_only"`
- `privacy.ui_full_profile_opt_in_default: false`

Behavior:

- Pipeline scoring sends only a redacted skills/experience summary.
- Personal contact details (email/phone) are stripped from AI profile context.
- In the UI, full profile sharing is OFF by default and requires explicit opt-in
  in AI settings.
