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


class GetRootRequest(BaseModel):
    user_id: str


class ReadTableRequest(BaseModel):
    table_name: str


class InsertRowWithTableRequest(BaseModel):
    table_name: str
    values: Dict[str, str]


class UpdateRowWithTableRequest(BaseModel):
    table_name: str
    row_id: int
    column: str
    value: str


class DeleteRowWithTableRequest(BaseModel):
    table_name: str
    row_id: int


class AddColumnWithTableRequest(BaseModel):
    table_name: str
    col_name: str
    col_type: str


class DeleteColumnWithTableRequest(BaseModel):
    table_name: str
    col_name: str


class DeleteTableRequest(BaseModel):
    table_name: str


class GetFilesRequest(BaseModel):
    current_folder_id: str


class GetFoldersRequest(BaseModel):
    current_folder_id: str
