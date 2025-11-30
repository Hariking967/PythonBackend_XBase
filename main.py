from fastapi import FastAPI, HTTPException
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
async def api_create_folder(folder_name: str, parent_id: str):
    # converting this to match your CLI flow
    import builtins
    builtins.input = lambda _: folder_name if "Folder name" in _ else parent_id

    await create_folder()
    return {"status": "folder_created", "folder_name": folder_name}


# -------------------------------------------------------
# CREATE TABLE
# -------------------------------------------------------
@app.post("/table/create")
async def api_create_table(table_name: str, parent_id: str, columns: list[str]):
    # Patch interactive inputs
    answers = [table_name, parent_id, str(len(columns))] + [
        item for col in columns for item in (col.split(":")[0], col.split(":")[1])
    ]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await create_table()
    return {"status": "table_created", "table_name": table_name}


# -------------------------------------------------------
# READ TABLE
# -------------------------------------------------------
@app.get("/table/read/{table_name}")
async def api_read_table(table_name: str):
    import builtins
    builtins.input = lambda _: table_name

    result = await read_rows()
    return {"table": table_name, "rows": result}


# -------------------------------------------------------
# INSERT ROW
# -------------------------------------------------------
@app.post("/table/insert/{table_name}")
async def api_insert_row(table_name: str, values: dict):
    answers = [table_name] + list(values.values())

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await insert_row()
    return {"status": "row_inserted", "table": table_name}


# -------------------------------------------------------
# UPDATE ROW
# -------------------------------------------------------
@app.put("/table/update/{table_name}")
async def api_update_row(table_name: str, row_id: int, column: str, value: str):
    answers = [table_name, str(row_id), column, value]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await update_row()
    return {"status": "row_updated", "table": table_name}


# -------------------------------------------------------
# DELETE ROW
# -------------------------------------------------------
@app.delete("/table/delete_row/{table_name}")
async def api_delete_row(table_name: str, row_id: int):
    answers = [table_name, str(row_id)]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await delete_row()
    return {"status": "row_deleted", "table": table_name}


# -------------------------------------------------------
# ADD COLUMN
# -------------------------------------------------------
@app.post("/table/add_column/{table_name}")
async def api_add_column(table_name: str, col_name: str, col_type: str):
    answers = [table_name, col_name, col_type]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await add_column()
    return {"status": "column_added", "table": table_name}


# -------------------------------------------------------
# DELETE COLUMN
# -------------------------------------------------------
@app.delete("/table/delete_column/{table_name}")
async def api_delete_column(table_name: str, col_name: str):
    answers = [table_name, col_name]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await delete_column()
    return {"status": "column_deleted", "table": table_name}


# -------------------------------------------------------
# DELETE TABLE
# -------------------------------------------------------
@app.delete("/table/{table_name}")
async def api_delete_table(table_name: str):
    answers = [table_name]

    import builtins
    iterator = iter(answers)
    builtins.input = lambda _: next(iterator)

    await delete_table()
    return {"status": "table_deleted", "table": table_name}
