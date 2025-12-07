import os
from dotenv import load_dotenv
load_dotenv()  # loads .env from project root
SYNC_DATABASE_URL = os.getenv("SYNC_DATABASE_URL")
from sqlalchemy import create_engine, text
engine = create_engine(SYNC_DATABASE_URL, future=True)
def run_sql(query: str):
    try:
        with engine.begin() as conn:
            result = conn.execute(text(query))
            try:
                return result.fetchall()
            except:
                return None
    except Exception as e:
        print("SQL ERROR:", e)
        return None