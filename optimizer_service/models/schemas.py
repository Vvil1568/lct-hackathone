from typing import List, Dict, Any, Optional

from pydantic import BaseModel, Field


class DDLStatement(BaseModel):
    statement: str

class QueryStatement(BaseModel):
    queryid: str
    query: str
    runquantity: int
    executiontime: int = 1

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

class ProfiledQuery(BaseModel):
    """Хранит всю собранную информацию об одном запросе."""
    queryid: str
    sql: str
    run_quantity: int
    execution_time: int
    cost: float = 0.0
    explain_plan: Dict[str, Any]
    tables: List[str] = Field(default_factory=list)

class DetectionResult(BaseModel):
    """
    Структура для хранения результатов работы детектора.
    """
    pattern_name: str
    message: str
    priority: int
    queries: List[ProfiledQuery]
    detector_name: str

class GlobalAnalysisReport(BaseModel):
    """Хранит финальный отчет глобального анализа."""
    top_cost_queries: List[ProfiledQuery]
    analysis_summary: str
    top_detection: Optional[DetectionResult] = None