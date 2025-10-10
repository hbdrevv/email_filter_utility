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

# ---- Patch gradio_client schema parser (handles additionalProperties: true/false) ----
try:
    import gradio_client.utils as _gc_utils  # type: ignore
    _orig_get_type = _gc_utils.get_type  # keep original for fallback

    def _safe_get_type(schema):
        # Some versions of gradio/gradio_client may pass bare booleans in schema positions.
        # Treat those as "any" to avoid: TypeError: argument of type 'bool' is not iterable
        if isinstance(schema, bool):
            return "any"
        return _orig_get_type(schema)

    _gc_utils.get_type = _safe_get_type  # monkey-patch
except Exception:
    pass

# ---- Email helpers ----
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
    # Punycode the domain if needed (IDN-safe)
    try:
        domain = idna.encode(domain).decode("ascii")
    except Exception:
        pass
    # Normalize common Gmail quirks
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
    """Read CSV/XLSX with a tolerant fallback."""
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

# ---- Core filtering ----
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

        # Prefer explicit column, otherwise autodetect
        email_col_name = (email_col_name or "").strip()
        client_email_col = email_col_name or autodetect_email_col(client_df.columns)
        supp_email_col = email_col_name or autodetect_email_col(supp_df.columns)
        if not client_email_col or not supp_email_col:
            return "❌ Could not detect the email column. Please enter it explicitly.", None, None

        # Build suppression set (canonicalized)
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

        # Masks on the ORIGINAL client_df (do not drop yet)
        empty_or_invalid_mask = client_df["_emails_list"].apply(lambda lst: len(lst) == 0)

        # Duplicate detection within the client file (by canonical primary email)
        # Mark any *later* occurrences as duplicates.
        dup_within_client_mask = client_df.duplicated(subset=["_primary_email"], keep="first")

        # Compute reason for removal with priority order:
        # 1) suppression, 2) empty/invalid (if option), 3) duplicate within client (if option)
        def removal_reason(row, idx):
            # suppression first (strongest reason)
            if any(em in suppression_set for em in row["_emails_list"]):
                return "suppression_match"
            # invalid/empty
            if drop_invalid_or_empty and empty_or_invalid_mask.iloc[idx]:
                return "empty_or_invalid_email"
            # duplicate within client
            if dedupe_before and dup_within_client_mask.iloc[idx]:
                return "duplicate_in_client"
            return ""

        # Evaluate reasons row-by-row
        reasons = []
        # preserve positional index for mask lookups
        for i, r in client_df.iterrows():
            reasons.append(removal_reason(r, client_df.index.get_loc(i)))
        reasons = pd.Series(reasons, index=client_df.index)

        removal_mask = reasons != ""
        removed_rows = client_df[removal_mask].drop(columns=["_emails_list", "_primary_email"]).copy()
        kept_rows = client_df[~removal_mask].drop(columns=["_emails_list", "_primary_email"]).copy()

        # Add reason column to removed rows for audit
        removed_rows.insert(0, "Reason", reasons[removal_mask].values)

        # Breakdown counts
        suppression_removed = int((reasons == "suppression_match").sum())
        empty_removed = int((reasons == "empty_or_invalid_email").sum())
        duplicate_removed = int((reasons == "duplicate_in_client").sum())

        # Outputs
        out_filtered = io.BytesIO(); kept_rows.to_csv(out_filtered, index=False)
        out_removed = io.BytesIO(); removed_rows.to_csv(out_removed, index=False)

        msg = (
            "✅ Done.\n"
            f"- Rows before filter: {before_total:,}\n"
            f"- Removed total: {int(removal_mask.sum()):,}\n"
            f"  • Suppression matches: {suppression_removed:,}\n"
            f"  • Empty/invalid emails: {empty_removed:,}\n"
            f"  • Duplicates in client: {duplicate_removed:,}\n"
            f"- Kept (ready to upload): {len(kept_rows):,}\n"
            f"- Gmail canonicalization: plus={'ON' if collapse_gmail_plus else 'OFF'}, "
            f"dots={'ON' if collapse_gmail_dots else 'OFF'}"
        )

        return msg, make_download(out_filtered, "client_list_filtered.csv"), make_download(out_removed, "removed_rows.csv")

    except Exception as e:
        traceback.print_exc()
        return f"❌ {type(e).__name__}: {e}", None, None

# ---- UI ----
with gr.Blocks(title="Email Filter", analytics_enabled=False) as demo:
    gr.Markdown("## 📧 Email Filter\nUpload your **Client List** and **Klaviyo Suppressions** (CSV or Excel).")
    with gr.Row():
        client_file = gr.File(label="Client List (CSV or XLSX)", file_types=[".csv", ".xlsx"])
        supp_file = gr.File(label="Klaviyo Suppressions (CSV or XLSX)", file_types=[".csv", ".xlsx"])
    email_col = gr.Textbox(label="Email column name (optional)", placeholder="e.g., Email or Email Address")
    with gr.Accordion("Options", open=False):
        dedupe_before = gr.Checkbox("Dedupe client list (remove duplicates within client file)", value=True)
        collapse_gmail_plus = gr.Checkbox("Treat Gmail plus aliases as the same", value=True)
        collapse_gmail_dots = gr.Checkbox("Treat Gmail dots as the same (a.lex → alex)", value=False)
        drop_invalid_or_empty = gr.Checkbox("Remove invalid/empty emails", value=True)
    run_btn = gr.Button("Filter & Prepare Downloads", variant="primary")
    status = gr.Markdown()
    dl_filtered = gr.File(label="Download: Filtered Client List", interactive=False)
    dl_removed = gr.File(label="Download: Removed Rows (Audit)", interactive=False)
    run_btn.click(
        filter_emails,
        inputs=[client_file, supp_file, email_col, dedupe_before, collapse_gmail_plus, collapse_gmail_dots, drop_invalid_or_empty],
        outputs=[status, dl_filtered, dl_removed]
    )

# ---- Launch ----
if __name__ == "__main__":
    # Toggle share link: GRADIO_SHARE=1 python email_filter_app.py
    share = os.getenv("GRADIO_SHARE") == "1"

    # Fixed port or auto-pick: set PORT to a number (e.g., 7861) or 0 (auto).
    port_env = os.getenv("PORT", "")
    server_port = int(port_env) if port_env.isdigit() else 0  # 0 = find free port

    try:
        demo.launch(
            server_name="127.0.0.1",
            server_port=server_port if server_port > 0 else None,  # None -> auto-pick
            show_error=True,
            inbrowser=False,
            share=share,
        )
    except ValueError as e:
        # Auto-fallback if localhost is blocked by a proxy/VPN
        if "localhost is not accessible" in str(e):
            print("⚠️ Localhost appears blocked. Relaunching with share=True ...")
            demo.launch(
                server_name="0.0.0.0",
                server_port=server_port if server_port > 0 else None,
                show_error=True,
                inbrowser=False,
                share=True,
            )
        else:
            raise
