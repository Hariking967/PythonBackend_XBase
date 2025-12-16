from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, text
import uuid
from datetime import datetime
from AskAI import Ask_AI
from python_runner.runner import run_code
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schemas import RunCodeRequest, RunCodeResponse
import subprocess
import json
import sys
import asyncio
import os
from CRUD import (
    get_or_create_user_root,
    create_folder,
    create_table,
    read_rows,
    insert_row,
    update_row,
    delete_row,
    add_column,
    delete_column,
    delete_table,
)
from schemas import (
    CreateFolderRequest, CreateTableRequest,
    GetRootRequest, ReadTableRequest,
    InsertRowWithTableRequest, UpdateRowWithTableRequest, DeleteRowWithTableRequest,
    AddColumnWithTableRequest, DeleteColumnWithTableRequest, DeleteTableRequest,
    GetFilesRequest, GetFoldersRequest,
    FilesCreateRequest, AskAISchema,  # added
    GetColumnsRequest, GetRowsRequest
)
from ConnectToDB import AsyncSessionLocal
from models import File, Folder
from runner import run_any
from RunSQL import run_sql

app = FastAPI(title="XBASE API", version="1.0")

# -------------------------------------------------------
# 🔥 CORS SETTINGS (THE FIX)
# -------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all (or replace with your Vercel domain)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------------
# USER ROOT (POST)
# -------------------------------------------------------
@app.post("/root")
async def api_get_or_create_user_root(body: GetRootRequest):
    root_id = await get_or_create_user_root(body.user_id)
    return {"user_id": body.user_id, "root_id": str(root_id)}


# -------------------------------------------------------
# CREATE FOLDER
# -------------------------------------------------------
@app.post("/folder/create")
async def api_create_folder(body: CreateFolderRequest):
    await create_folder(body.folder_name, body.parent_id)
    return {"status": "folder_created", "folder_name": body.folder_name}


# -------------------------------------------------------
# CREATE TABLE
# -------------------------------------------------------
@app.post("/table/create")
async def api_create_table(body: CreateTableRequest):
    await create_table(body.table_name, body.parent_id, body.columns)
    return {"status": "table_created", "table_name": body.table_name}


# -------------------------------------------------------
# READ TABLE
# -------------------------------------------------------
@app.post("/table/read")
async def api_read_table(body: ReadTableRequest):
    rows = await read_rows(body.table_name)
    return {"table": body.table_name, "rows": rows}


# -------------------------------------------------------
# INSERT ROW
# -------------------------------------------------------
@app.post("/table/insert")
async def api_insert_row(body: InsertRowWithTableRequest):
    await insert_row(body.table_name, body.values)
    return {"status": "row_inserted", "table": body.table_name}


# -------------------------------------------------------
# UPDATE ROW
# -------------------------------------------------------
@app.post("/table/update")
async def api_update_row(body: UpdateRowWithTableRequest):
    await update_row(body.table_name, body.row_id, body.column, body.value)
    return {"status": "row_updated", "table": body.table_name}


# -------------------------------------------------------
# DELETE ROW
# -------------------------------------------------------
@app.post("/table/delete_row")
async def api_delete_row(body: DeleteRowWithTableRequest):
    await delete_row(body.table_name, body.row_id)
    return {"status": "row_deleted", "table": body.table_name}


# -------------------------------------------------------
# ADD COLUMN
# -------------------------------------------------------
@app.post("/table/add_column")
async def api_add_column(body: AddColumnWithTableRequest):
    await add_column(body.table_name, body.col_name, body.col_type)
    return {"status": "column_added", "table": body.table_name}


# -------------------------------------------------------
# DELETE COLUMN
# -------------------------------------------------------
@app.post("/table/delete_column")
async def api_delete_column(body: DeleteColumnWithTableRequest):
    await delete_column(body.table_name, body.col_name)
    return {"status": "column_deleted", "table": body.table_name}


# -------------------------------------------------------
# DELETE TABLE
# -------------------------------------------------------
@app.post("/table/delete")
async def api_delete_table(body: DeleteTableRequest):
    await delete_table(body.table_name)
    return {"status": "table_deleted", "table": body.table_name}


# -------------------------------------------------------
# GET FILES IN FOLDER
# -------------------------------------------------------
@app.post("/files")
async def api_get_files(body: GetFilesRequest):
    parent_uuid = uuid.UUID(body.current_folder_id)

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(File).where(File.parent_id == parent_uuid))
        files = result.scalars().all()

    return {
        "files": [
            {
                "id": str(f.id),
                "name": f.name,
                "parent_id": str(f.parent_id),
                "created_at": f.created_at.isoformat() if f.created_at else None,
                "bucket_url": getattr(f, "bucket_url", None),
            }
            for f in files
        ]
    }


# -------------------------------------------------------
# GET FOLDERS IN FOLDER
# -------------------------------------------------------
@app.post("/folders")
async def api_get_folders(body: GetFoldersRequest):
    parent_uuid = uuid.UUID(body.current_folder_id)

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Folder).where(Folder.parent_id == parent_uuid))
        folders = result.scalars().all()

    return {
        "folders": [
            {
                "id": str(d.id),
                "name": d.name,
                "parent_id": str(d.parent_id),
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in folders
        ]
    }


# -------------------------------------------------------
# CREATE FILE (POST)
# -------------------------------------------------------
@app.post("/files/create")
async def api_create_file(body: FilesCreateRequest):
    file_id = uuid.uuid4()
    created_at = datetime.utcnow()
    
    # Ensure parent_id is UUID
    parent_id = uuid.UUID(body.current_folder_id)

    async with AsyncSessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO files (id, name, created_at, parent_id, bucket_url)
                VALUES (:id, :name, :created_at, :parent_id, :bucket_url)
            """),
            {
                "id": str(file_id),
                "name": body.name,
                "created_at": created_at,
                "parent_id": str(parent_id),
                "bucket_url": body.bucket_url,
            },
        )
        await session.commit()

    # Return full file info matching your frontend schema
    return {
        "status": "file_created",
        "file": {
            "id": str(file_id),
            "name": body.name,
            "created_at": created_at.isoformat(),
            "parent_id": str(parent_id),
            "bucket_url": body.bucket_url,
        },
    }

# @app.post("/ask_ai")
# def ask_ai_endpoint(payload: AskAISchema):
#     """
#     FastAPI endpoint that wraps your Ask_AI() function.
#     Accepts db_info, query, chat_history.
#     Returns the AI's response and the updated chat_history.
#     """

#     # Ensure chat_history is a list (FastAPI will give none if not provided)
#     history = payload.chat_history or []

#     result = Ask_AI(
#         db_info=payload.db_info,
#         query=payload.query,
#         chat_history=history,
#         parent_id=payload.parent_id
#     )

#     # Return updated history as well so client can persist it
#     # Ask_AI now returns (final, chat_history, image_box, sql_res)
#     final_text, updated_history, image_box, sql_res = result
#     return {
#         "response": final_text,
#         "chat_history": updated_history,
#         "images": image_box,
#         "sql_res": sql_res
#     }

@app.post("/ask_ai")
def ask_ai_endpoint(payload: AskAISchema):
    """
    AI query endpoint.

    Expects:
    - db_info
    - query
    - chat_history (optional)
    - parent_id

    Returns:
    - response (str)
    - chat_history (list)
    - images (list)
    - sql_res (any)
    """

    try:
        # Normalize history
        history = payload.chat_history or []

        # Call core AI logic
        result = Ask_AI(
            db_info=payload.db_info,
            query=payload.query,
            chat_history=history,
            parent_id=payload.parent_id
        )

        # ---- MANUAL UNPACKING (EXPLICIT & SAFE) ----
        final_text, updated_history, image_box, sql_res, _ = result

        return {
            "response": final_text,
            "chat_history": updated_history,
            "images": image_box,
            "sql_res": sql_res
        }

    except ValueError as e:
        # Contract mismatch / unpacking error
        raise HTTPException(
            status_code=500,
            detail=f"Ask_AI return contract mismatch: {str(e)}"
        )

    except Exception as e:
        # Any other runtime error inside Ask_AI
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Ask_AI execution failed: {str(e)}"
        )

RUNNER_PATH = os.path.join(
    os.path.dirname(__file__),
    "python_runner",
    "runner.py"
)
import concurrent.futures

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

def run_runner_subprocess(payload: bytes):
    """Runs runner.py synchronously inside a separate thread."""
    process = subprocess.Popen(
        [sys.executable, RUNNER_PATH],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    stdout, stderr = process.communicate(payload)

    return stdout.decode("utf-8").strip(), stderr.decode("utf-8").strip()


# @app.post("/run", response_model=RunCodeResponse)
# async def run_code(request: RunCodeRequest):

#     payload = json.dumps({
#         "code": request.code,
#         "bucket_url": request.bucket_url
#     }).encode("utf-8")

#     loop = asyncio.get_running_loop()

#     # Run subprocess in a separate thread (Windows safe)
#     stdout, stderr = await loop.run_in_executor(
#         executor,
#         run_runner_subprocess,
#         payload
#     )

#     if not stdout:
#         raise HTTPException(500, f"Runner error: {stderr}")

#     try:
#         result = json.loads(stdout)
#     except Exception:
#         raise HTTPException(500, f"Invalid JSON from runner: {stdout}")

#     return RunCodeResponse(
#         output=result.get("output"),
#         error=result.get("error"),
#         images=result.get("images", []),
#         bucket_url=result.get("bucket_url", request.bucket_url),
#         csv_text=result.get("csv_text")
#     )
@app.post("/run", response_model=RunCodeResponse)
async def run_code(request: RunCodeRequest):
    payload = json.dumps({
        "code": request.code,
        "bucket_url": request.bucket_url
    }).encode("utf-8")

    loop = asyncio.get_running_loop()

    stdout, stderr = await loop.run_in_executor(
        executor,
        run_runner_subprocess,
        payload
    )

    if not stdout:
        raise HTTPException(
            status_code=500,
            detail=f"Runner produced no output. stderr:\n{stderr}"
        )

    try:
        result = json.loads(stdout)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "Invalid JSON from runner.\n"
                f"stdout:\n{stdout}\n\nstderr:\n{stderr}"
            )
        )

    return RunCodeResponse(
        output=result.get("output"),
        error=result.get("error") or stderr or None,
        images=result.get("images") or [],
        bucket_url=result.get("bucket_url", request.bucket_url),
        sql_res=None
    )

# -------------------------------------------------------
# GET COLUMNS (SYNC via SYNC_DATABASE_URL)
# -------------------------------------------------------
@app.post("/getColumns")
def get_columns(req: GetColumnsRequest):
    import re
    # sanitize table name to avoid injection in f-string
    if not re.match(r"^[A-Za-z0-9_]+$", req.table_name):
        raise HTTPException(status_code=400, detail="Invalid table_name")

    # derive schema name (consistent with existing pattern)
    schema_name = "schema" + req.parent_id.replace('-', '_')

    # set schema and fetch column names
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    run_sql(f"SET search_path TO {schema_name}")

    cols = run_sql(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = current_schema() AND table_name = '{req.table_name}' "
        f"ORDER BY ordinal_position;"
    ) or []

    return {"columns": [c[0] for c in cols]}


# -------------------------------------------------------
# GET ROWS (SYNC via SYNC_DATABASE_URL)
# -------------------------------------------------------
@app.post("/getRows")
def get_rows(req: GetRowsRequest):
    import re
    if not re.match(r"^[A-Za-z0-9_]+$", req.table_name):
        raise HTTPException(status_code=400, detail="Invalid table_name")

    schema_name = "schema" + req.parent_id.replace('-', '_')
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    run_sql(f"SET search_path TO {schema_name}")

    rows = run_sql(f"SELECT * FROM {req.table_name};") or []
    # convert tuples to lists for JSON serialization
    try:
        rows_list = [list(r) for r in rows]
    except Exception:
        rows_list = rows

    return {"rows": rows_list}