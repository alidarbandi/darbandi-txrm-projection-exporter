# Publishing To GitHub

This repository is ready to publish, but the current machine needs Git or GitHub CLI authentication first.

## Option 1: Publish With GitHub Desktop

1. Open GitHub Desktop.
2. Choose `File` -> `Add local repository`.
3. Select this folder:

   ```text
   C:\Users\ali darbandi\Documents\Codex\txrm_reader
   ```

4. If prompted, initialize it as a Git repository.
5. Commit the files.
6. Click `Publish repository`.
7. Recommended repository name:

   ```text
   darbandi-txrm-projection-exporter
   ```

## Option 2: Publish With Git And GitHub CLI

After installing Git and GitHub CLI, run:

```powershell
cd "C:\Users\ali darbandi\Documents\Codex\txrm_reader"
git init
git add .gitignore CONTRIBUTING.md LICENSE PUBLISHING.md README.md docs requirements.txt pyproject.toml txrm_read.py
git commit -m "Initial release of Darbandi TXRM Projection Exporter"
gh auth login
gh repo create darbandi-txrm-projection-exporter --private --source . --remote origin --push
```

Use `--public` instead of `--private` if you want the project public.

## Option 3: Publish From GitHub Website

1. Create a new repository on GitHub named `darbandi-txrm-projection-exporter`.
2. Upload these files and folders:
   - `.gitignore`
   - `CONTRIBUTING.md`
   - `LICENSE`
   - `PUBLISHING.md`
   - `README.md`
   - `docs/`
   - `requirements.txt`
   - `pyproject.toml`
   - `txrm_read.py`

Do not upload scan data, generated TIFF files, metadata outputs, logs, or `__pycache__`.
