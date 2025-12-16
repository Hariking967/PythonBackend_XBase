from pydantic import BaseModel
from typing import List, Dict, Optional

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


class FilesCreateRequest(BaseModel):
    current_folder_id: str
    name: str
    bucket_url: str

class AskAISchema(BaseModel):
    db_info: str        
    query: str          
    chat_history: List  
    parent_id: str
    # optional image_box can be provided by client; server will return images separately
    

# class RunCodeRequest(BaseModel):
#     code: str
#     bucket_url: str


# class RunCodeResponse(BaseModel):
#     output: Optional[str]
#     error: Optional[str]
#     images: List[str] | None = []
#     bucket_url: str | None = None
#     # For ask_ai endpoint response extension
#     sql_res: Optional[List] = None

class RunCodeRequest(BaseModel):
    code: str
    bucket_url: str


class RunCodeResponse(BaseModel):
    output: Optional[str] = None
    error: Optional[str] = None
    images: List[str] = Field(default_factory=list)
    bucket_url: Optional[str] = None
    sql_res: Optional[List] = None


# -----------------------------
# New: Sync data fetch schemas
# -----------------------------
class GetColumnsRequest(BaseModel):
    parent_id: str
    table_name: str

class GetRowsRequest(BaseModel):
    parent_id: str
    table_name: str
