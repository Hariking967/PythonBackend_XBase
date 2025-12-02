from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
import uuid

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
)
from ConnectToDB import AsyncSessionLocal
from models import File, Folder

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
