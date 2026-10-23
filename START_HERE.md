# Finish and submit the project

This is an **add-on package for your existing working project**, not a standalone
Docker stack. Your working `dags/nairobi_air_quality.py`, `docker-compose.yaml`
and `sql/create_tables.sql` remain the source of truth. They were reviewed in
chat and are deliberately not included as replacements here.

## 1. Copy these complete files into the WSL project

Download and extract `nairobi-submission-pack.zip`. In WSL, open the project:

```bash
cd ~/nairobi-air-quality-airflow
explorer.exe .
```

Copy the contents of the extracted `nairobi-submission` folder into this project
folder. Include `.gitignore` and `.env.example`. If you already have a README,
tests, requirements or ignore rules, back them up and merge useful existing
content before replacing. Keep the actual `.env` and existing runtime logs.
This package contains no actual `.env` and no replacement DAG or Compose file.

It adds tests, documentation, evidence screenshots, data-quality SQL, placeholders,
requirements documentation, an example environment file and ignore rules.
Review the final Git status to ensure your actual project files are included too.

## 2. Run the packaged tests

```bash
docker compose exec airflow-scheduler mkdir -p /tmp/nairobi-project-tests
docker compose cp ./tests/. airflow-scheduler:/tmp/nairobi-project-tests/
docker compose exec -T airflow-scheduler python -m unittest discover -s /tmp/nairobi-project-tests -p 'test_*.py' -v
```

Expected: 17 tests, then `OK`. These mocked tests do not call OpenAQ or write
warehouse data. If any fail, retain the full output and resolve the failure
before claiming they pass. Copy stdout/stderr into `docs/test-results.txt` if
you want a durable transcript; a screenshot is also useful.

After running successfully, update the README's test-result wording with the
actual result and date. The supplied README currently states accurately that
the packaged suite still needs execution in your installation.

## 3. Check the remaining evidence

`docs/evidence.md` indexes the screenshots and all four scenarios. The direct
validation test terminal output was supplied in chat, so it is transcribed;
its original screenshot was not supplied. The original idempotency before/after
figures were author-recorded observations. Demonstrate row counts live during
the presentation as required by the assignment.

## 4. Initialize Git and check what will be committed

Run from the project root. `git init` also works if the folder is already a repo.

```bash
git init
git status --short
git remote -v
git check-ignore .env logs/example.log
git check-ignore .env.example
```

The first ignore check should print `.env` and `logs/example.log`. The
`.env.example` check should print nothing (exit status 1 is expected).
If `.env` or real logs were already tracked, ignore rules alone will not remove
them from Git history. Resolve any prior secret exposure before publishing.
Do not share real API keys, Connection exports or `.env` in the repository.

Stage the required files explicitly:

```bash
git add dags sql tests docs README.md START_HERE.md docker-compose.yaml requirements.txt .env.example .gitignore config/.gitkeep logs/.gitkeep
git diff --cached --check
git diff --cached --stat
git diff --cached
```

Inspect the staged content privately. The included demo credentials are from the
local Compose setup; the actual OpenAQ key must not appear. Confirm logs only
contains `.gitkeep`, `.env` is not staged, and `.env.example` is staged.

```bash
git status --short
git commit -m "Complete Nairobi air quality pipeline, tests and evidence"
```

If Git asks for your identity, configure your actual author name and GitHub
email locally, then retry the commit. Do not copy a made-up email.

## 5. Push to GitHub

Check `git remote -v` first. If the correct remote already exists, push the
current branch with `git push -u origin HEAD`.

If no remote exists, create an empty repository named
`nairobi-air-quality-airflow` under `Kungu11` on GitHub. Do not initialize it with
a second README or license. Choose visibility according to submission rules;
a private repo needs assessor access. Then run:

```bash
git remote add origin https://github.com/Kungu11/nairobi-air-quality-airflow.git
git push -u origin HEAD
```

Do not force-push over an existing repository. If the target already contains
work or the remote differs, inspect it before changing history. This package
does not create or push a GitHub repository automatically.

## 6. Present and submit

Read `docs/presentation.md` and rehearse the live row-count comparison. Use
`docs/technical_writeup.md` as the short technical write-up. GitHub renders the
Mermaid diagram in `docs/architecture.md`.

Final required structure: `dags/`, `sql/`, `config/`, `logs/`, `tests/`,
`docker-compose.yaml`, `requirements.txt`, `.env.example`, `.gitignore`, `README.md`.
The evidence and write-up live under `docs/`. Submit the actual repository URL
and give the assessor access. Never delete named volumes during preparation.
