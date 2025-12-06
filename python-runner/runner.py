import sys, io, json

def run_any(code):
    # capture stdout
    buffer = io.StringIO()
    sys.stdout = buffer

    try:
        exec(code, {}, {})
        return {"output": buffer.getvalue(), "error": None}
    except Exception as e:
        return {"output": None, "error": str(e)}
    finally:
        sys.stdout = sys.__stdout__

payload = json.loads(sys.stdin.read())
code = payload["code"]

result = run_any(code)

print(json.dumps(result))
