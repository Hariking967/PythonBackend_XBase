from pydantic import BaseModel
from typing import List, Dict


class CreateFolderRequest(BaseModel):
    folder_name: str
    parent_id: str


class CreateTableRequest(BaseModel):
    table_name: str
    parent_id: str
    columns: List[str]   # Example: ["name:TEXT", "age:INT"]


class InsertRowRequest(BaseModel):
    values: Dict[str, str]


class UpdateRowRequest(BaseModel):
    row_id: int
    column: str
    value: str


class DeleteRowRequest(BaseModel):
    row_id: int


class AddColumnRequest(BaseModel):
    col_name: str
    col_type: str


class DeleteColumnRequest(BaseModel):
    col_name: str
