# email_filter_app.py
import io
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Iterable, Optional, Set

import idna
import pandas as pd
import gradio as gr

# Candidate email headers for auto-detection
CANDIDATE_EMAIL_COLS = [
    "email", "e-mail", "Email", "EMAIL",
    "Email Address", "email_address", "EmailAddress", "user_email"
]

EMAIL_TOKENIZER = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

def autodetect_email_col(cols: Iterable[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for c in CANDIDATE_EMAIL_COLS:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    for c in cols:
        if "mail" in c.lower():
            return c
    return None

def extract_emails(cell: object) -> list[str]:
    if pd.isna(cell):
        return []
    s = unicodedata.normalize("NFKC", str(cell))
    return EMAIL_TOKENIZER.findall(s)

def canonicalize_email(raw_email: str, collapse_gmail_plus=True, collapse_gmail_dots=False) -> str:
    e = unicodedata.normalize("NFKC", raw_email).strip().lower()
    if "@" not in e:
        return e
    local, domain = e.split("@", 1)
    try:
        domain = idna.encode(domain).decode("ascii")
    except Exception:
        pass
    if domain in ("gmail.com", "googlemail.com"):
        if collapse_gmail_plus and "+" in local:
            local = local.split("+", 1)[0]
        if collapse_gmail_dots:
            local = local.replace(".", "")
    return f"{local}@{domain}"

def canonicalize_cell_to_list(cell: object, collapse_gmail_plus: bool, collapse_gmail_dots: bool) -> list[str]:
    return [
        canonicalize_email(e, collapse_gmail_plus, collapse_gmail_dots)
        for e in extract_emails(cell)
        if e
    ]

def read_table(file_obj) -> pd.DataFrame:
    name = getattr(file_obj, "name", None) or getattr(file_obj, "orig_name", None)
    suffix = Path(name).suffix.lower() if name else ""
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_obj)
    if suffix == ".csv":
        return pd.read_csv(file_obj)
    # fallback
    try:
        return pd.read_csv(file_obj)
    except Exception:
        return pd.read_excel(file_obj)

def make_download(bytes_io: io.BytesIO, filename: str):
    bytes_io.seek(0)
    tmpdir = tempfile.gettempdir()
    out_path = os.path.join(tmpdir, filename)
    with open(out_path, "wb") as f:
        f.write(bytes_io.read())
    return out_path

def filter_emails(client_file, supp_file, email_col_name, dedupe_before,
                  collapse_gmail_plus, collapse_gmail_dots, drop_invalid_or_empty):
    import traceback
    try:
        if client_file is None or supp_file is None:
            return "Please upload both files.", None, None

        client_df = read_table(client_file)
        supp_df = read_table(supp_file)

        if client_df.empty:
            return "❌ Client file has no rows.", None, None
        if supp_df.empty:
            return "❌ Suppression file has no rows.", None, None

        # Detect email columns
        email_col_name = (email_col_name or "").strip()
        client_email_col = email_col_name or autodetect_email_col(client_df.columns)
        supp_email_col = email_col_name or autodetect_email_col(supp_df.columns)
        if not client_email_col or not supp_email_col:
            return "❌ Could not detect the email column.", None, None

        # Suppression set
        suppression_set: Set[str] = set()
        for cell in supp_df[supp_email_col].tolist():
            suppression_set.update(
                canonicalize_cell_to_list(cell, collapse_gmail_plus, collapse_gmail_dots)
            )

        # Canonicalize client emails
        client_df = client_df.copy()
        client_df["_emails_list"] = client_df[client_email_col].apply(
            lambda x: canonicalize_cell_to_list(x, collapse_gmail_plus, collapse_gmail_dots)
        )
        client_df["_primary_email"] = client_df["_emails_list"].apply(lambda lst: lst[0] if lst else "")

        before_total = len(client_df)

        if dedupe_before:
            client_df = client_df.drop_duplicates(subset=["_primary_email"], keep="first")

        def row_is_removed(row) -> bool:
            if drop_invalid_or_empty and not row["_emails_list"]:
                return True
            return any(em in suppression_set for em in row["_emails_list"])

        removal_mask = client_df.apply(row_is_removed, axis=1)
        removed_rows = client_df[removal_mask].drop(columns=["_emails_list", "_primary_email"])
        kept_rows = client_df[~removal_mask].drop(columns=["_emails_list", "_primary_email"])

        out_filtered = io.BytesIO(); kept_rows.to_csv(out_filtered, index=False)
        out_removed = io.BytesIO(); removed_rows.to_csv(out_removed, index=False)

        msg = (
            f"✅ Done.\n"
            f"- Rows before filter: {before_total:,}\n"
            f"- Removed: {len(removed_rows):,}\n"
            f"- Kept: {len(kept_rows):,}\n"
            f"- Gmail canonicalization: plus={'ON' if collapse_gmail_plus else 'OFF'}, "
            f"dots={'ON' if collapse_gmail_dots else 'OFF'}"
        )

        return msg, make_download(out_filtered, "client_list_filtered.csv"), make_download(out_removed, "removed_rows.csv")

    except Exception as e:
        traceback.print_exc()
        return f"❌ {type(e).__name__}: {e}", None, None

with gr.Blocks(title="Email Filter") as demo:
    gr.Markdown("## 📧 Email Filter\nUpload your **Client List** and **Klaviyo Suppressions** (CSV or Excel).")
    with gr.Row():
        client_file = gr.File(label="Client List (CSV or XLSX)", file_types=[".csv", ".xlsx"])
        supp_file = gr.File(label="Klaviyo Suppressions (CSV or XLSX)", file_types=[".csv", ".xlsx"])
    email_col = gr.Textbox(label="Email column name (optional)", placeholder="e.g., Email or Email Address")
    with gr.Accordion("Options", open=False):
        dedupe_before = gr.Checkbox("Dedupe client list", value=True)
        collapse_gmail_plus = gr.Checkbox("Treat Gmail plus aliases as the same", value=True)
        collapse_gmail_dots = gr.Checkbox("Treat Gmail dots as the same (a.lex → alex)", value=False)
        drop_invalid_or_empty = gr.Checkbox("Remove invalid/empty emails", value=True)
    run_btn = gr.Button("Filter & Prepare Downloads", variant="primary")
    status = gr.Markdown()
    dl_filtered = gr.File(label="Download: Filtered Client List", interactive=False)
    dl_removed = gr.File(label="Download: Removed Rows (Audit)", interactive=False)
    run_btn.click(filter_emails,
                  inputs=[client_file, supp_file, email_col, dedupe_before, collapse_gmail_plus, collapse_gmail_dots, drop_invalid_or_empty],
                  outputs=[status, dl_filtered, dl_removed])

if __name__ == "__main__":
    demo.launch()
