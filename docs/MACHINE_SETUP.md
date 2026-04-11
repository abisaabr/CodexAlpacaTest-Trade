# Machine Setup

## Windows

1. Install Python 3.11 or newer.
2. Run `powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_windows.ps1`
3. Copy `.env.example` to `.env` if it was not created automatically.
4. Add paper-trading credentials only to `.env`.
5. Run `python scripts/doctor.py`

## macOS

1. Install Python 3.11 or newer.
2. Run `bash ./scripts/bootstrap_mac.sh`
3. Copy `.env.example` to `.env` if needed.
4. Add paper-trading credentials only to `.env`.
5. Run `python scripts/doctor.py`

## Linux

1. Install Python 3.11 or newer.
2. Run `bash ./scripts/bootstrap_linux.sh`
3. Copy `.env.example` to `.env` if needed.
4. Add paper-trading credentials only to `.env`.
5. Run `python scripts/doctor.py`

## Notes

- This repo is public. Never paste secrets into code, docs, issues, or workflows.
- All runtime outputs stay in local `data/` and `reports/`, which are ignored by git.
- The QQQ paper portfolio expects a repo-local virtualenv at `.venv\Scripts\python.exe` on Windows. `scripts\bootstrap_windows.ps1` creates it.
- To enable the daily automated QQQ session on Windows, install the scheduled task with `powershell -ExecutionPolicy Bypass -File .\scripts\install_qqq_paper_task.ps1 -TaskName "QQQ Portfolio Paper Trader" -StartTime "09:20"`.
