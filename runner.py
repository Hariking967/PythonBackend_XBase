import io
import sys

def run_any(code: str, file_content: str | None = None):
    """Executes arbitrary Python code with optional CSV content (available as 'csv_content')."""
    buffer = io.StringIO()
    sys_stdout_original = sys.stdout
    sys.stdout = buffer
    try:
        exec(code, {"csv_content": file_content}, {})
        output = buffer.getvalue()
        return {"output": output, "error": None}
    except Exception as e:
        return {"output": None, "error": str(e)}
    finally:
        sys.stdout = sys_stdout_original
