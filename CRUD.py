import asyncio
from sqlalchemy import text
from ConnectToDB import engine, AsyncSessionLocal
from models import File
import uuid
from models import UserRoot
from sqlalchemy import select
import uuid

# ------------------------------------------------
# CHECK OR CREATE USER ROOT
# ------------------------------------------------
async def get_or_create_user_root(user_id: str):
    async with AsyncSessionLocal() as session:

        # 1️⃣ Check if user already has a root
        result = await session.execute(
            select(UserRoot).where(UserRoot.user_id == user_id)
        )
        user_root = result.scalars().first()

        if user_root:
            # Already exists → return existing root UUID
            print(f"User root already exists for {user_id}: {user_root.root}")
            return user_root.root

        # 2️⃣ Not found → create a new root
        new_root_uuid = uuid.uuid4()

        new_entry = UserRoot(
            id=uuid.uuid4(),
            user_id=user_id,
            root=new_root_uuid
        )

        session.add(new_entry)
        await session.commit()

        print(f"Created new root for {user_id}: {new_root_uuid}")
        return new_root_uuid

# ----------------------------------------
# Helper: Execute a SQL query
# ----------------------------------------
async def run_sql(query):
    async with engine.begin() as conn:
        result = await conn.execute(text(query))
        try:
            return result.fetchall()
        except:
            return None


# ----------------------------------------
# CREATE TABLE + Register in File ORM
# ----------------------------------------
async def create_table():
    table_name = input("Enter table name: ")
    parent_id = input("Enter parent folder UUID: ")

    num_cols = int(input("Enter number of columns: "))

    columns = []
    for i in range(num_cols):
        col_name = input(f"Column {i+1} name: ")
        col_type = input("Type (TEXT, INT, FLOAT): ").upper()
        columns.append(f"{col_name} {col_type}")

    col_sql = ", ".join(columns)

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {col_sql},
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """

    await run_sql(create_query)

    async with AsyncSessionLocal() as session:
        new_file = File(
            id=uuid.uuid4(),
            name=table_name,
            parent_id=uuid.UUID(parent_id)
        )
        session.add(new_file)
        await session.commit()

    print(f"Table '{table_name}' created and registered!\n")


# ----------------------------------------
# READ ROWS
# ----------------------------------------
async def read_rows():
    table = input("Table name: ")
    query = f"SELECT * FROM {table};"
    rows = await run_sql(query)

    if not rows:
        print("No rows or table is empty.\n")
        return

    for row in rows:
        print(row)
    print("\n")


# ----------------------------------------
# INSERT ROW
# ----------------------------------------
async def insert_row():
    table = input("Table name: ")

    cols_res = await run_sql(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='{table}' AND column_name NOT IN ('id', 'created_at');
    """)

    if not cols_res:
        print("Table does not exist or has no columns.\n")
        return

    cols = [c[0] for c in cols_res]

    data = {}
    for col in cols:
        val = input(f"Enter value for {col}: ")
        data[col] = val

    col_names = ", ".join(data.keys())
    col_vals = ", ".join([f"'{v}'" for v in data.values()])

    query = f"""
    INSERT INTO {table} ({col_names})
    VALUES ({col_vals});
    """

    await run_sql(query)
    print("Row inserted!\n")


# ----------------------------------------
# UPDATE ROW
# ----------------------------------------
async def update_row():
    table = input("Table name: ")
    row_id = input("ID of row to update: ")
    column = input("Column to update: ")
    value = input("New value: ")

    query = f"""
    UPDATE {table}
    SET {column} = '{value}'
    WHERE id = {row_id};
    """

    await run_sql(query)
    print("Row updated!\n")


# ----------------------------------------
# DELETE ROW
# ----------------------------------------
async def delete_row():
    table = input("Table name: ")
    row_id = input("ID of row to delete: ")

    query = f"DELETE FROM {table} WHERE id = {row_id};"
    await run_sql(query)
    print("Row deleted!\n")


# ----------------------------------------
# ADD COLUMN
# ----------------------------------------
async def add_column():
    table = input("Table name: ")
    col_name = input("New column name: ")
    col_type = input("Column type (TEXT, INT, FLOAT): ").upper()

    query = f"""
    ALTER TABLE {table}
    ADD COLUMN {col_name} {col_type};
    """

    await run_sql(query)
    print("Column added!\n")


# ----------------------------------------
# DELETE COLUMN
# ----------------------------------------
async def delete_column():
    table = input("Table name: ")
    col = input("Column name to delete: ")

    query = f"""
    ALTER TABLE {table}
    DROP COLUMN {col};
    """

    await run_sql(query)
    print("Column deleted!\n")


# ----------------------------------------
# DELETE TABLE
# ----------------------------------------
async def delete_table():
    table = input("Table name to delete: ")

    drop_query = f"DROP TABLE IF EXISTS {table} CASCADE;"
    await run_sql(drop_query)

    async with AsyncSessionLocal() as session:
        await session.execute(
            text("DELETE FROM files WHERE name = :t"),
            {"t": table}
        )
        await session.commit()

    print(f"Table '{table}' dropped and file entry removed!\n")


# ----------------------------------------
# SAFE UUID
# ----------------------------------------
def safe_uuid(u):
    try:
        return uuid.UUID(u)
    except:
        print("❌ Invalid UUID format!")
        return None


# ----------------------------------------
# CREATE FOLDER (No restrictions)
# ----------------------------------------
async def create_folder():
    print("\n--- Create New Folder ---")

    folder_name = input("Folder name: ")
    parent_str = input("Parent folder UUID: ")

    parent_uuid = safe_uuid(parent_str)
    if parent_uuid is None:
        return

    async with AsyncSessionLocal() as session:
        from models import Folder
        new_folder = Folder(
            id=uuid.uuid4(),
            name=folder_name,
            parent_id=parent_uuid
        )
        session.add(new_folder)
        await session.commit()

    print(f"Folder '{folder_name}' created!\n")


# ----------------------------------------
# MENU LOOP
# ----------------------------------------
async def menu():
    while True:
        print("==== XBASE DB MENU ====")
        print("0. Create Folder")
        print("1. Create Table")
        print("2. Read Rows")
        print("3. Insert Row")
        print("4. Update Row")
        print("5. Delete Row")
        print("6. Add Column")
        print("7. Delete Column")
        print("8. Delete Table (with file cleanup)")
        print("9. Exit")
        print("========================")

        choice = input("Choose an option: ")

        if choice == "0":
            await create_folder()
        elif choice == "1":
            await create_table()
        elif choice == "2":
            await read_rows()
        elif choice == "3":
            await insert_row()
        elif choice == "4":
            await update_row()
        elif choice == "5":
            await delete_row()
        elif choice == "6":
            await add_column()
        elif choice == "7":
            await delete_column()
        elif choice == "8":
            await delete_table()
        elif choice == "9":
            print("Goodbye!")
            break
        else:
            print("Invalid choice.\n")


asyncio.run(menu())
