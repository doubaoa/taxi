"""
查询相关的API端点
"""
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import json
import asyncio
from datetime import datetime
import structlog

from app.agents.nl2sql_agent import NL2SQLAgent
from app.agents.query_optimizer_agent import QueryOptimizerAgent
from app.agents.result_analyzer_agent import ResultAnalyzerAgent
from app.models.query_context import QueryContext
from app.services.database_service import DatabaseService
from app.services.visualization_service import VisualizationService
from app.core.deps import get_current_user, get_database_service
from app.core.security import check_query_permissions

logger = structlog.get_logger()

router = APIRouter()

# 请求模型
class NLQueryRequest(BaseModel):
    """自然语言查询请求"""
    query: str = Field(..., description="自然语言查询")
    database_name: str = Field(..., description="数据库名称")
    context_id: Optional[str] = Field(None, description="上下文ID")
    stream: bool = Field(False, description="是否流式响应")
    max_results: int = Field(1000, description="最大结果数量")
    visualization_type: Optional[str] = Field(None, description="可视化类型")

class SQLQueryRequest(BaseModel):
    """SQL查询请求"""
    sql: str = Field(..., description="SQL查询语句")
    database_name: str = Field(..., description="数据库名称")
    explain_plan: bool = Field(False, description="是否返回执行计划")
    cache_results: bool = Field(True, description="是否缓存结果")

class QueryOptimizationRequest(BaseModel):
    """查询优化请求"""
    sql: str = Field(..., description="待优化的SQL")
    database_name: str = Field(..., description="数据库名称")
    optimization_level: str = Field("medium", description="优化级别: low, medium, high")

# 响应模型
class QueryResponse(BaseModel):
    """查询响应"""
    query_id: str
    natural_query: Optional[str] = None
    sql: str
    results: List[Dict[str, Any]]
    total_rows: int
    execution_time: float
    explanation: Optional[str] = None
    visualizations: Optional[List[Dict[str, Any]]] = None
    suggestions: Optional[List[str]] = None
    timestamp: datetime

class QueryAnalysisResponse(BaseModel):
    """查询分析响应"""
    intent: Dict[str, Any]
    complexity_score: float
    estimated_cost: float
    security_warnings: List[str]
    optimization_suggestions: List[Dict[str, Any]]

# WebSocket连接管理
class ConnectionManager:
    """WebSocket连接管理器"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# Agent实例
nl2sql_agent = NL2SQLAgent()
optimizer_agent = QueryOptimizerAgent()
analyzer_agent = ResultAnalyzerAgent()

@router.post("/nl-query", response_model=QueryResponse)
async def process_natural_language_query(
    request: NLQueryRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends(get_database_service)
):
    """处理自然语言查询"""
    
    try:
        # 检查权限
        await check_query_permissions(current_user, request.database_name)
        
        # 创建查询上下文
        context = QueryContext(
            user_id=current_user["id"],
            database_name=request.database_name,
            context_id=request.context_id or f"ctx_{datetime.utcnow().timestamp()}"
        )
        
        logger.info("Processing NL query", 
                   query=request.query, 
                   user_id=current_user["id"],
                   database=request.database_name)
        
        # 1. NL2SQL转换
        start_time = datetime.utcnow()
        
        nl2sql_result = await nl2sql_agent.process_query(
            natural_query=request.query,
            context=context,
            stream=request.stream
        )
        
        sql = nl2sql_result["sql"]
        
        # 2. 执行SQL查询
        execution_start = datetime.utcnow()
        
        query_result = await db_service.execute_query(
            sql=sql,
            database_name=request.database_name,
            max_results=request.max_results
        )
        
        execution_time = (datetime.utcnow() - execution_start).total_seconds()
        
        # 3. 结果分析和可视化（后台任务）
        if request.visualization_type:
            background_tasks.add_task(
                generate_visualizations,
                query_result["data"],
                request.visualization_type,
                context
            )
        
        # 4. 构建响应
        response = QueryResponse(
            query_id=context.context_id,
            natural_query=request.query,
            sql=sql,
            results=query_result["data"],
            total_rows=query_result["total_rows"],
            execution_time=execution_time,
            explanation=nl2sql_result.get("explanation"),
            timestamp=datetime.utcnow()
        )
        
        logger.info("Successfully processed NL query",
                   query_id=context.context_id,
                   execution_time=execution_time,
                   total_rows=query_result["total_rows"])
        
        return response
        
    except Exception as e:
        logger.error("Error processing NL query",
                    query=request.query,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sql-query", response_model=QueryResponse)
async def execute_sql_query(
    request: SQLQueryRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends(get_database_service)
):
    """执行SQL查询"""
    
    try:
        # 检查权限
        await check_query_permissions(current_user, request.database_name)
        
        # 安全检查
        if not nl2sql_agent.security_checker.is_safe_sql(request.sql):
            raise HTTPException(status_code=400, detail="SQL contains unsafe operations")
        
        logger.info("Executing SQL query",
                   sql=request.sql,
                   user_id=current_user["id"],
                   database=request.database_name)
        
        # 执行查询
        start_time = datetime.utcnow()
        
        query_result = await db_service.execute_query(
            sql=request.sql,
            database_name=request.database_name,
            explain_plan=request.explain_plan,
            cache_results=request.cache_results
        )
        
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        
        # 生成解释（后台任务）
        explanation = None
        if request.explain_plan:
            background_tasks.add_task(
                generate_sql_explanation,
                request.sql,
                current_user["id"]
            )
        
        response = QueryResponse(
            query_id=f"sql_{datetime.utcnow().timestamp()}",
            sql=request.sql,
            results=query_result["data"],
            total_rows=query_result["total_rows"],
            execution_time=execution_time,
            explanation=explanation,
            timestamp=datetime.utcnow()
        )
        
        logger.info("Successfully executed SQL query",
                   execution_time=execution_time,
                   total_rows=query_result["total_rows"])
        
        return response
        
    except Exception as e:
        logger.error("Error executing SQL query",
                    sql=request.sql,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze-query", response_model=QueryAnalysisResponse)
async def analyze_query(
    request: SQLQueryRequest,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends(get_database_service)
):
    """分析SQL查询"""
    
    try:
        # 检查权限
        await check_query_permissions(current_user, request.database_name)
        
        logger.info("Analyzing query",
                   sql=request.sql,
                   user_id=current_user["id"])
        
        # 创建查询上下文
        context = QueryContext(
            user_id=current_user["id"],
            database_name=request.database_name
        )
        
        # 分析查询
        analysis_result = await analyzer_agent.analyze_query(
            sql=request.sql,
            context=context
        )
        
        response = QueryAnalysisResponse(
            intent=analysis_result["intent"],
            complexity_score=analysis_result["complexity_score"],
            estimated_cost=analysis_result["estimated_cost"],
            security_warnings=analysis_result["security_warnings"],
            optimization_suggestions=analysis_result["optimization_suggestions"]
        )
        
        return response
        
    except Exception as e:
        logger.error("Error analyzing query",
                    sql=request.sql,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/optimize-query")
async def optimize_query(
    request: QueryOptimizationRequest,
    current_user: Dict = Depends(get_current_user)
):
    """优化SQL查询"""
    
    try:
        # 检查权限
        await check_query_permissions(current_user, request.database_name)
        
        logger.info("Optimizing query",
                   sql=request.sql,
                   optimization_level=request.optimization_level)
        
        # 创建查询上下文
        context = QueryContext(
            user_id=current_user["id"],
            database_name=request.database_name
        )
        
        # 优化查询
        optimization_result = await optimizer_agent.optimize_query(
            sql=request.sql,
            context=context,
            optimization_level=request.optimization_level
        )
        
        return optimization_result
        
    except Exception as e:
        logger.error("Error optimizing query",
                    sql=request.sql,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/query-stream/{database_name}")
async def query_stream(
    websocket: WebSocket,
    database_name: str,
    token: str
):
    """WebSocket流式查询"""
    
    try:
        # 验证token和权限
        current_user = await verify_websocket_token(token)
        await check_query_permissions(current_user, database_name)
        
        await manager.connect(websocket)
        
        logger.info("WebSocket connection established",
                   user_id=current_user["id"],
                   database=database_name)
        
        try:
            while True:
                # 接收查询请求
                data = await websocket.receive_text()
                query_request = json.loads(data)
                
                # 创建查询上下文
                context = QueryContext(
                    user_id=current_user["id"],
                    database_name=database_name,
                    context_id=f"ws_{datetime.utcnow().timestamp()}"
                )
                
                # 流式处理查询
                async for chunk in stream_query_processing(
                    query_request["query"],
                    context,
                    websocket
                ):
                    await manager.send_personal_message(
                        json.dumps(chunk),
                        websocket
                    )
                    
        except WebSocketDisconnect:
            manager.disconnect(websocket)
            logger.info("WebSocket connection closed",
                       user_id=current_user["id"])
            
    except Exception as e:
        logger.error("WebSocket error",
                    error=str(e),
                    exc_info=True)
        await websocket.close(code=1011, reason=str(e))

@router.get("/query-history/{user_id}")
async def get_query_history(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends(get_database_service)
):
    """获取查询历史"""
    
    try:
        # 权限检查：只能查看自己的历史
        if current_user["id"] != user_id and not current_user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Permission denied")
        
        history = await db_service.get_query_history(
            user_id=user_id,
            limit=limit,
            offset=offset
        )
        
        return history
        
    except Exception as e:
        logger.error("Error fetching query history",
                    user_id=user_id,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/query-history/{query_id}")
async def delete_query_from_history(
    query_id: str,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends(get_database_service)
):
    """删除查询历史记录"""
    
    try:
        # 检查查询是否属于当前用户
        query_info = await db_service.get_query_info(query_id)
        if query_info["user_id"] != current_user["id"] and not current_user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Permission denied")
        
        await db_service.delete_query_history(query_id)
        
        return {"message": "Query deleted from history"}
        
    except Exception as e:
        logger.error("Error deleting query history",
                    query_id=query_id,
                    error=str(e),
                    exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# 后台任务函数
async def generate_visualizations(
    data: List[Dict[str, Any]],
    visualization_type: str,
    context: QueryContext
):
    """生成可视化图表（后台任务）"""
    
    try:
        viz_service = VisualizationService()
        
        visualizations = await viz_service.generate_charts(
            data=data,
            chart_type=visualization_type,
            context=context
        )
        
        # 将结果存储到缓存或数据库
        cache_key = f"viz:{context.context_id}"
        await viz_service.cache_service.set(
            cache_key,
            json.dumps(visualizations),
            ttl=3600
        )
        
        logger.info("Generated visualizations",
                   context_id=context.context_id,
                   chart_count=len(visualizations))
        
    except Exception as e:
        logger.error("Error generating visualizations",
                    context_id=context.context_id,
                    error=str(e),
                    exc_info=True)

async def generate_sql_explanation(sql: str, user_id: str):
    """生成SQL解释（后台任务）"""
    
    try:
        explanation = await nl2sql_agent.explain_sql(sql)
        
        # 存储解释结果
        cache_key = f"explanation:{hash(sql)}:{user_id}"
        await nl2sql_agent.cache_service.set(
            cache_key,
            explanation,
            ttl=86400  # 24小时
        )
        
        logger.info("Generated SQL explanation",
                   user_id=user_id,
                   sql_hash=hash(sql))
        
    except Exception as e:
        logger.error("Error generating SQL explanation",
                    user_id=user_id,
                    error=str(e),
                    exc_info=True)

async def stream_query_processing(
    query: str,
    context: QueryContext,
    websocket: WebSocket
):
    """流式查询处理"""
    
    try:
        # 发送状态更新
        yield {"status": "analyzing", "message": "正在分析查询..."}
        
        # NL2SQL转换
        nl2sql_result = await nl2sql_agent.process_query(
            natural_query=query,
            context=context,
            stream=True
        )
        
        yield {
            "status": "sql_generated",
            "sql": nl2sql_result["sql"],
            "explanation": nl2sql_result.get("explanation", "")
        }
        
        # 执行查询
        yield {"status": "executing", "message": "正在执行查询..."}
        
        db_service = DatabaseService()
        result = await db_service.execute_query(
            sql=nl2sql_result["sql"],
            database_name=context.database_name
        )
        
        # 发送结果
        yield {
            "status": "completed",
            "results": result["data"],
            "total_rows": result["total_rows"]
        }
        
    except Exception as e:
        yield {
            "status": "error",
            "error": str(e)
        }

async def verify_websocket_token(token: str) -> Dict:
    """验证WebSocket token"""
    # 这里应该实现token验证逻辑
    # 返回用户信息
    pass