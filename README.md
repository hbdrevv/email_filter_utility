#Early Utility that inspired a browser based solution (RinseList)

I used this to help clean client lists quickly. Checking new data against current suppression lists is easy but can become time consuming or disruptive to other workflows. This was a sipmle solution to reduce my cognitive load but the question quickly arose "how can we share this with clients?" If we can make the task easy for even the least technical clients, we can empower them and reduce our task load while creating a positive helpful experience. 

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
