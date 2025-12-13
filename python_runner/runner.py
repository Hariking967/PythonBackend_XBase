# runner.py
import json
import sys
import io
import traceback
import base64
import pandas as pd
import csv
import os
from supabase import create_client, Client
from io import StringIO
from dotenv import load_dotenv

# ---------------------------------------------------------
# Basic env loader (kept here but get_supabase also reloads)
# ---------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://fzhnmrpzumoqpcfciatk.supabase.co")
BUCKET_NAME = "XBase_bucket1"
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


# ---------------------------------------------------------
# Lazy Supabase client loader (no crash on import)
# ---------------------------------------------------------
def get_supabase():
    # Ensure .env is loaded inside subprocess / runtime
    load_dotenv()
    url = os.getenv("SUPABASE_URL") or SUPABASE_URL
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or SUPABASE_SERVICE_ROLE_KEY

    if not url or not key:
        raise Exception("Supabase credentials missing (SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY).")

    return create_client(url, key)


# ---------------------------------------------------------
# Extract any matplotlib figures as base64 for return
# ---------------------------------------------------------
def extract_images():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return []

    images = []
    figs = list(map(plt.figure, plt.get_fignums()))
    for fig in figs:
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        images.append(base64.b64encode(buf.read()).decode())
        plt.close(fig)

    return images


# ---------------------------------------------------------
# Smart CSV Delimiter Detection + Parsing (keeps your logic)
# returns (df, error_msg)
# ---------------------------------------------------------
def smart_csv_to_df(csv_text: str):
    if not csv_text or csv_text.strip() == "":
        return None, "CSV is empty"

    try:
        # Remove BOM if present
        if csv_text.startswith("\ufeff"):
            csv_text = csv_text.encode().decode("utf-8-sig")

        sample = csv_text[:5000]
        sniffer = csv.Sniffer()

        # Detect delimiter
        try:
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
        except Exception:
            if ";" in sample:
                delimiter = ";"
            elif "|" in sample:
                delimiter = "|"
            elif "\t" in sample:
                delimiter = "\t"
            else:
                delimiter = ","

        # Try python engine
        try:
            df = pd.read_csv(
                io.StringIO(csv_text),
                sep=delimiter,
                engine="python",
                on_bad_lines="skip"
            )
            return df, None
        except Exception:
            pass

        # Fallback: C-engine
        try:
            df = pd.read_csv(
                io.StringIO(csv_text),
                sep=delimiter,
                engine="c",
                on_bad_lines="skip"
            )
            return df, None
        except Exception as e:
            return None, f"Failed to parse CSV: {str(e)}"

    except Exception as e:
        return None, f"smart_csv_to_df error: {str(e)}"


# ---------------------------------------------------------
# Robust fetch: handle many possible 'download' return types
# Returns (df) or None on failure. Also prints debug info to stderr.
# ---------------------------------------------------------
def load_csv_from_supabase(bucket_path: str):
    """
    Returns: pandas.DataFrame or None
    """
    if not bucket_path:
        print("DEBUG: load_csv_from_supabase called with empty bucket_path", file=sys.stderr)
        return None

    try:
        supabase = get_supabase()
    except Exception as e:
        print("DEBUG: get_supabase() error:", str(e), file=sys.stderr)
        return None

    try:
        print("DEBUG: Supabase download called for:", bucket_path, file=sys.stderr)
        res = supabase.storage.from_(BUCKET_NAME).download(bucket_path)

        # Debug print the returned type/value summary
        try:
            tname = type(res).__name__
            summary = None
            if isinstance(res, (bytes, bytearray)):
                summary = f"bytes len={len(res)}"
            elif hasattr(res, "content"):
                summary = f"has .content len={len(res.content)}"
            elif isinstance(res, (tuple, list)):
                summary = f"tuple/list len={len(res)} types={[type(x).__name__ for x in res]}"
            elif isinstance(res, dict):
                summary = f"dict keys={list(res.keys())}"
            else:
                summary = str(res)[:200]
            print(f"DEBUG: download result type={tname} summary={summary}", file=sys.stderr)
        except Exception:
            print("DEBUG: download result introspect failed", file=sys.stderr)

        raw_bytes = None

        # Handle bytes-like directly
        if isinstance(res, (bytes, bytearray)):
            raw_bytes = bytes(res)

        # requests-like Response
        elif hasattr(res, "content"):
            raw_bytes = res.content

        # file-like (has read)
        elif hasattr(res, "read") and callable(res.read):
            raw_bytes = res.read()

        # tuple/list returned (some versions return (data, error) or (data, response))
        elif isinstance(res, (tuple, list)) and len(res) >= 1:
            first = res[0]
            if isinstance(first, (bytes, bytearray)):
                raw_bytes = bytes(first)
            elif isinstance(first, dict) and "data" in first:
                data = first["data"]
                if isinstance(data, (bytes, bytearray)):
                    raw_bytes = bytes(data)
                elif isinstance(data, str):
                    raw_bytes = data.encode("utf-8")
            elif isinstance(first, str):
                raw_bytes = first.encode("utf-8")
            elif hasattr(first, "content"):
                raw_bytes = first.content
            else:
                # try to stringify the first element
                try:
                    raw_bytes = str(first).encode("utf-8")
                except Exception:
                    raw_bytes = None

        # dict with 'data' (some clients)
        elif isinstance(res, dict) and "data" in res:
            data = res["data"]
            if isinstance(data, (bytes, bytearray)):
                raw_bytes = bytes(data)
            elif isinstance(data, str):
                raw_bytes = data.encode("utf-8")

        # fallback: convert to str
        else:
            try:
                raw_bytes = str(res).encode("utf-8")
            except Exception:
                raw_bytes = None

        if not raw_bytes:
            print("DEBUG: Unable to extract bytes from supabase.download result. raw_bytes is None", file=sys.stderr)
            return None

        # Try decode to text
        try:
            text = raw_bytes.decode("utf-8")
        except Exception:
            text = raw_bytes.decode("utf-8", errors="ignore")

        # Parse to dataframe using your smart parser (delimiter detection)
        df, parse_err = smart_csv_to_df(text)
        if df is None:
            print("DEBUG: smart_csv_to_df failed:", parse_err, file=sys.stderr)
            return None

        return df

    except Exception as e:
        # Print full exception to stderr so FastAPI logs capture it
        print("DEBUG: Exception during Supabase SDK fetch:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return None


# ---------------------------------------------------------
# Execute Python Code with df injected
# ---------------------------------------------------------
def run_code(code: str, bucket_url: str):
    local_ns = {}

    df = load_csv_from_supabase(bucket_url)

    # Ensure only a proper DataFrame is injected
    if isinstance(df, pd.DataFrame):
        local_ns["df"] = df
    else:
        # Provide None so user code can test `if df is None:`
        local_ns["df"] = None
        print("DEBUG: df is None after load_csv_from_supabase", file=sys.stderr)

    stdout_backup = sys.stdout
    stderr_backup = sys.stderr
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        # Execute the user's code with df injected
        exec(code, {"__name__": "__main__"}, local_ns)

        sys.stdout = stdout_backup
        sys.stderr = stderr_backup

        output = stdout_capture.getvalue()
        error = stderr_capture.getvalue()
        images = extract_images()

        return {
            "output": output,
            "error": error,
            "images": images,
            "bucket_url": bucket_url,
        }

    except Exception:
        sys.stdout = stdout_backup
        sys.stderr = stderr_backup
        return {
            "output": None,
            "error": traceback.format_exc(),
            "images": [],
            "bucket_url": bucket_url,
        }


# ---------------------------------------------------------
# Runner entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        payload = json.loads(sys.stdin.read())
        code = payload.get("code", "")
        bucket_url = payload.get("bucket_url", "")

        result = run_code(code, bucket_url)
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({
            "output": None,
            "error": str(e),
            "images": [],
            "csv_text": None
        }))
