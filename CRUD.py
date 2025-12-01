import uuid
from sqlalchemy import text, select
from sqlalchemy.exc import SQLAlchemyError
from ConnectToDB import engine, AsyncSessionLocal
from models import File, Folder, UserRoot


# ------------------------------------------------
# RUN RAW SQL
# ------------------------------------------------
async def run_sql(query: str):
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(query))
            try:
                return result.fetchall()
            except:
                return None
    except SQLAlchemyError as e:
        print("SQL ERROR:", e)
        return None


# ------------------------------------------------
# USER ROOT: Get or create root folder
# ------------------------------------------------
async def get_or_create_user_root(user_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserRoot).where(UserRoot.user_id == user_id)
        )
        user_root = result.scalars().first()

        if user_root:
            return user_root.root

        new_root_uuid = uuid.uuid4()

        new_entry = UserRoot(
            id=uuid.uuid4(),
            user_id=user_id,
            root=new_root_uuid
        )

        session.add(new_entry)
        await session.commit()

        return new_root_uuid


# ------------------------------------------------
# CREATE FOLDER
# ------------------------------------------------
async def create_folder(folder_name: str, parent_id: str):
    parent_uuid = uuid.UUID(parent_id)

    async with AsyncSessionLocal() as session:
        new_folder = Folder(
            id=uuid.uuid4(),
            name=folder_name,
            parent_id=parent_uuid
        )
        session.add(new_folder)
        await session.commit()

    return {"folder_id": str(new_folder.id), "name": folder_name}


# ------------------------------------------------
# CREATE TABLE + REGISTER IN FILE ORM
# ------------------------------------------------
async def create_table(table_name: str, parent_id: str, columns: list[str]):
    parent_uuid = uuid.UUID(parent_id)

    # Build SQL for dynamic table
    col_sql = ", ".join([f"{col.split(':')[0]} {col.split(':')[1]}" for col in columns])

    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {col_sql},
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """

    await run_sql(query)

    async with AsyncSessionLocal() as session:
        new_file = File(
            id=uuid.uuid4(),
            name=table_name,
            parent_id=parent_uuid
        )
        session.add(new_file)
        await session.commit()

    return {"table": table_name, "status": "created"}


# ------------------------------------------------
# READ ROWS
# ------------------------------------------------
async def read_rows(table_name: str):
    query = f"SELECT * FROM {table_name};"
    rows = await run_sql(query)
    return rows or []


# ------------------------------------------------
# INSERT ROW
# ------------------------------------------------
async def insert_row(table_name: str, data: dict):
    col_names = ", ".join(data.keys())
    col_vals = ", ".join([f"'{v}'" for v in data.values()])

    query = f"""
    INSERT INTO {table_name} ({col_names})
    VALUES ({col_vals});
    """

    await run_sql(query)
    return {"status": "inserted", "table": table_name}


# ------------------------------------------------
# UPDATE ROW
# ------------------------------------------------
async def update_row(table_name: str, row_id: int, column: str, value: str):
    query = f"""
    UPDATE {table_name}
    SET {column} = '{value}'
    WHERE id = {row_id};
    """

    await run_sql(query)
    return {"status": "updated", "table": table_name}


# ------------------------------------------------
# DELETE ROW
# ------------------------------------------------
async def delete_row(table_name: str, row_id: int):
    query = f"DELETE FROM {table_name} WHERE id = {row_id};"
    await run_sql(query)
    return {"status": "deleted", "table": table_name}


# ------------------------------------------------
# ADD COLUMN
# ------------------------------------------------
async def add_column(table_name: str, col_name: str, col_type: str):
    query = f"""
    ALTER TABLE {table_name}
    ADD COLUMN {col_name} {col_type};
    """

    await run_sql(query)
    return {"status": "column_added", "column": col_name}


# ------------------------------------------------
# DELETE COLUMN
# ------------------------------------------------
async def delete_column(table_name: str, col_name: str):
    query = f"""
    ALTER TABLE {table_name}
    DROP COLUMN {col_name};
    """

    await run_sql(query)
    return {"status": "column_deleted", "column": col_name}


# ------------------------------------------------
# DELETE TABLE (And remove from File ORM)
# ------------------------------------------------
async def delete_table(table_name: str):
    drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
    await run_sql(drop_query)

    async with AsyncSessionLocal() as session:
        await session.execute(
            text("DELETE FROM files WHERE name = :t"),
            {"t": table_name}
        )
        await session.commit()

    return {"status": "table_deleted", "table": table_name}
