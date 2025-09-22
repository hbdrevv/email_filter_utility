# Email Filter (Python/Gradio)

A local tool to remove rows from a client list if the email appears in a suppression file.

- Works with CSV or Excel (.xlsx)
- All processing happens locally on the user’s machine
- No uploads, no external services
- Includes sample data in `/examples`

## Quick start

```bash
git clone https://github.com/yourname/email-filter.git
cd email-filter
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python email_filter_app.py
