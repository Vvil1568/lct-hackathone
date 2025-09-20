from pydantic import BaseModel
from typing import List, Dict, Any


class DDLStatement(BaseModel):
    statement: str

class QueryStatement(BaseModel):
    queryid: str
    query: str
    runquantity: int

class TaskRequest(BaseModel):
    url: str
    ddl: List[DDLStatement]
    queries: List[QueryStatement]



class TaskResponse(BaseModel):
    taskid: str

class TaskStatus(BaseModel):
    status: str
    

class OptimizationResult(BaseModel):
    ddl: List[Dict[str, Any]]
    migrations: List[Dict[str, Any]]
    queries: List[Dict[str, Any]]