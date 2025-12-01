from fastapi import FastAPI
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
    delete_table
)
from schemas import (
    CreateFolderRequest, CreateTableRequest, InsertRowRequest,
    UpdateRowRequest, DeleteRowRequest, AddColumnRequest, DeleteColumnRequest
)

app = FastAPI(title="XBASE API", version="1.0")


# -------------------------------------------------------
# USER ROOT
# -------------------------------------------------------
@app.get("/root/{user_id}")
async def api_get_or_create_user_root(user_id: str):
    root_id = await get_or_create_user_root(user_id)
    return {"user_id": user_id, "root_id": str(root_id)}


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
@app.get("/table/read/{table_name}")
async def api_read_table(table_name: str):
    rows = await read_rows(table_name)
    return {"table": table_name, "rows": rows}


# -------------------------------------------------------
# INSERT ROW
# -------------------------------------------------------
@app.post("/table/insert/{table_name}")
async def api_insert_row(table_name: str, body: InsertRowRequest):
    await insert_row(table_name, body.values)
    return {"status": "row_inserted", "table": table_name}


# -------------------------------------------------------
# UPDATE ROW
# -------------------------------------------------------
@app.put("/table/update/{table_name}")
async def api_update_row(table_name: str, body: UpdateRowRequest):
    await update_row(table_name, body.row_id, body.column, body.value)
    return {"status": "row_updated", "table": table_name}


# -------------------------------------------------------
# DELETE ROW
# -------------------------------------------------------
@app.delete("/table/delete_row/{table_name}")
async def api_delete_row(table_name: str, body: DeleteRowRequest):
    await delete_row(table_name, body.row_id)
    return {"status": "row_deleted", "table": table_name}


# -------------------------------------------------------
# ADD COLUMN
# -------------------------------------------------------
@app.post("/table/add_column/{table_name}")
async def api_add_column(table_name: str, body: AddColumnRequest):
    await add_column(table_name, body.col_name, body.col_type)
    return {"status": "column_added", "table": table_name}


# -------------------------------------------------------
# DELETE COLUMN
# -------------------------------------------------------
@app.delete("/table/delete_column/{table_name}")
async def api_delete_column(table_name: str, body: DeleteColumnRequest):
    await delete_column(table_name, body.col_name)
    return {"status": "column_deleted", "table": table_name}


# -------------------------------------------------------
# DELETE TABLE
# -------------------------------------------------------
@app.delete("/table/{table_name}")
async def api_delete_table(table_name: str):
    await delete_table(table_name)
    return {"status": "table_deleted", "table": table_name}
