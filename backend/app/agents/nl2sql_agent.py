"""
NL2SQL Agent - 自然语言到SQL转换的核心Agent
"""
import asyncio
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import structlog

from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI, ChatAnthropic
from langchain.schema import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.callbacks import AsyncCallbackHandler
from langchain.memory import ConversationBufferWindowMemory

from app.core.config import settings
from app.models.database_schema import DatabaseSchema
from app.models.query_context import QueryContext, QueryIntent
from app.services.database_service import DatabaseService
from app.services.cache_service import CacheService
from app.utils.sql_validator import SQLValidator
from app.utils.security import SecurityChecker

logger = structlog.get_logger()


class StreamingCallbackHandler(AsyncCallbackHandler):
    """流式响应回调处理器"""
    
    def __init__(self):
        self.tokens = []
        
    async def on_llm_new_token(self, token: str, **kwargs) -> None:
        self.tokens.append(token)


class NL2SQLAgent:
    """自然语言到SQL转换Agent"""
    
    def __init__(self):
        self.db_service = DatabaseService()
        self.cache_service = CacheService()
        self.sql_validator = SQLValidator()
        self.security_checker = SecurityChecker()
        
        # 初始化大模型
        self._init_llm()
        
        # 初始化对话记忆
        self.memory = ConversationBufferWindowMemory(
            k=10,  # 保留最近10轮对话
            return_messages=True
        )
        
        # SQL模板库
        self.sql_templates = self._load_sql_templates()
        
    def _init_llm(self):
        """初始化大模型"""
        if settings.OPENAI_API_KEY:
            self.llm = ChatOpenAI(
                openai_api_key=settings.OPENAI_API_KEY,
                model_name=settings.OPENAI_MODEL,
                temperature=0.1,
                streaming=True,
                callbacks=[StreamingCallbackHandler()]
            )
        elif settings.ANTHROPIC_API_KEY:
            self.llm = ChatAnthropic(
                anthropic_api_key=settings.ANTHROPIC_API_KEY,
                model=settings.ANTHROPIC_MODEL,
                temperature=0.1
            )
        else:
            raise ValueError("No valid LLM API key provided")
    
    def _load_sql_templates(self) -> Dict[str, str]:
        """加载SQL模板库"""
        return {
            "basic_select": """
            SELECT {columns}
            FROM {table}
            {where_clause}
            {group_by}
            {order_by}
            {limit}
            """,
            
            "aggregation": """
            SELECT {group_columns}, {agg_functions}
            FROM {table}
            {where_clause}
            GROUP BY {group_columns}
            {having_clause}
            {order_by}
            {limit}
            """,
            
            "join": """
            SELECT {columns}
            FROM {main_table} {main_alias}
            {join_clauses}
            {where_clause}
            {group_by}
            {order_by}
            {limit}
            """,
            
            "time_series": """
            SELECT 
                DATE_TRUNC('{time_unit}', {time_column}) as time_period,
                {agg_functions}
            FROM {table}
            {where_clause}
            GROUP BY DATE_TRUNC('{time_unit}', {time_column})
            ORDER BY time_period
            {limit}
            """
        }
    
    async def process_query(
        self, 
        natural_query: str, 
        context: QueryContext,
        stream: bool = False
    ) -> Dict[str, Any]:
        """处理自然语言查询"""
        
        try:
            # 1. 查询缓存
            cache_key = f"nl2sql:{hash(natural_query + str(context.dict()))}"
            cached_result = await self.cache_service.get(cache_key)
            
            if cached_result:
                logger.info("Cache hit for query", query=natural_query)
                return json.loads(cached_result)
            
            # 2. 安全检查
            if not self.security_checker.is_safe_query(natural_query):
                raise ValueError("Query contains unsafe content")
            
            # 3. 意图识别和实体抽取
            intent_result = await self._analyze_intent(natural_query, context)
            
            # 4. 生成SQL
            sql_result = await self._generate_sql(
                natural_query, 
                intent_result, 
                context,
                stream=stream
            )
            
            # 5. SQL验证和优化
            validated_sql = await self._validate_and_optimize_sql(
                sql_result["sql"], 
                context
            )
            
            # 6. 构建最终结果
            result = {
                "natural_query": natural_query,
                "intent": intent_result,
                "sql": validated_sql,
                "explanation": sql_result.get("explanation", ""),
                "confidence": sql_result.get("confidence", 0.8),
                "timestamp": datetime.utcnow().isoformat(),
                "context_id": context.context_id
            }
            
            # 7. 缓存结果
            await self.cache_service.set(
                cache_key, 
                json.dumps(result), 
                ttl=settings.QUERY_CACHE_TTL
            )
            
            # 8. 更新对话记忆
            self.memory.chat_memory.add_user_message(natural_query)
            self.memory.chat_memory.add_ai_message(validated_sql)
            
            logger.info("Successfully processed NL2SQL query", 
                       query=natural_query, 
                       sql=validated_sql)
            
            return result
            
        except Exception as e:
            logger.error("Error processing NL2SQL query", 
                        query=natural_query, 
                        error=str(e),
                        exc_info=True)
            raise
    
    async def _analyze_intent(
        self, 
        natural_query: str, 
        context: QueryContext
    ) -> Dict[str, Any]:
        """意图识别和实体抽取"""
        
        intent_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="""
            你是一个专业的SQL查询意图分析专家。请分析用户的自然语言查询，识别查询意图和关键实体。

            请返回JSON格式的结果，包含以下字段：
            - intent_type: 查询类型 (select, aggregation, join, time_series, etc.)
            - entities: 识别出的实体
              - tables: 涉及的表名
              - columns: 涉及的字段名
              - conditions: 过滤条件
              - time_range: 时间范围
              - aggregations: 聚合函数
            - confidence: 置信度 (0-1)
            
            数据库Schema信息：
            {schema_info}
            """),
            HumanMessage(content=f"请分析这个查询：{natural_query}")
        ])
        
        # 获取数据库Schema信息
        schema_info = await self._get_schema_context(context)
        
        messages = intent_prompt.format_messages(schema_info=schema_info)
        response = await self.llm.agenerate([messages])
        
        try:
            intent_result = json.loads(response.generations[0][0].text)
            return intent_result
        except json.JSONDecodeError:
            # 如果解析失败，返回默认结果
            return {
                "intent_type": "select",
                "entities": {"tables": [], "columns": [], "conditions": []},
                "confidence": 0.5
            }
    
    async def _generate_sql(
        self, 
        natural_query: str,
        intent_result: Dict[str, Any],
        context: QueryContext,
        stream: bool = False
    ) -> Dict[str, Any]:
        """生成SQL查询"""
        
        # 构建Few-shot示例
        few_shot_examples = await self._get_few_shot_examples(
            intent_result["intent_type"]
        )
        
        # 获取对话历史
        conversation_history = self.memory.chat_memory.messages[-6:]  # 最近3轮对话
        
        sql_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content=f"""
            你是一个专业的SQL查询生成专家。请根据用户的自然语言查询生成准确的SQL语句。

            数据库信息：
            {await self._get_schema_context(context)}

            规则：
            1. 只生成SELECT查询，不允许修改数据的操作
            2. 使用标准SQL语法
            3. 适当使用表别名提高可读性
            4. 对于时间相关查询，优先使用索引字段
            5. 限制结果集大小，默认LIMIT 1000

            Few-shot示例：
            {few_shot_examples}

            对话历史：
            {self._format_conversation_history(conversation_history)}

            请返回JSON格式，包含：
            - sql: 生成的SQL语句
            - explanation: SQL语句的解释
            - confidence: 置信度 (0-1)
            """),
            HumanMessage(content=f"""
            用户查询：{natural_query}
            
            意图分析结果：{json.dumps(intent_result, ensure_ascii=False)}
            
            请生成相应的SQL查询。
            """)
        ])
        
        if stream:
            # 流式生成
            callback = StreamingCallbackHandler()
            response = await self.llm.agenerate(
                [sql_prompt.format_messages()],
                callbacks=[callback]
            )
        else:
            response = await self.llm.agenerate([sql_prompt.format_messages()])
        
        try:
            sql_result = json.loads(response.generations[0][0].text)
            return sql_result
        except json.JSONDecodeError:
            # 解析失败时的fallback
            generated_text = response.generations[0][0].text
            sql_match = re.search(r'```sql\n(.*?)\n```', generated_text, re.DOTALL)
            
            if sql_match:
                sql = sql_match.group(1).strip()
            else:
                sql = generated_text.strip()
            
            return {
                "sql": sql,
                "explanation": "Generated SQL query",
                "confidence": 0.7
            }
    
    async def _validate_and_optimize_sql(
        self, 
        sql: str, 
        context: QueryContext
    ) -> str:
        """验证和优化SQL"""
        
        # 1. 语法验证
        if not self.sql_validator.validate_syntax(sql):
            raise ValueError("Generated SQL has syntax errors")
        
        # 2. 安全检查
        if not self.security_checker.is_safe_sql(sql):
            raise ValueError("Generated SQL contains unsafe operations")
        
        # 3. 性能优化
        optimized_sql = await self.sql_validator.optimize_query(sql, context)
        
        # 4. 添加安全限制
        if "LIMIT" not in optimized_sql.upper():
            optimized_sql += " LIMIT 1000"
        
        return optimized_sql
    
    async def _get_schema_context(self, context: QueryContext) -> str:
        """获取数据库Schema上下文"""
        
        # 从缓存或数据库获取Schema信息
        cache_key = f"schema:{context.database_name}"
        cached_schema = await self.cache_service.get(cache_key)
        
        if cached_schema:
            return cached_schema
        
        schema_info = await self.db_service.get_database_schema(
            context.database_name
        )
        
        # 格式化Schema信息
        formatted_schema = self._format_schema_info(schema_info)
        
        # 缓存Schema信息
        await self.cache_service.set(
            cache_key, 
            formatted_schema, 
            ttl=settings.METADATA_CACHE_TTL
        )
        
        return formatted_schema
    
    def _format_schema_info(self, schema_info: List[Dict]) -> str:
        """格式化Schema信息"""
        formatted_lines = []
        
        for table in schema_info:
            table_name = table["table_name"]
            columns = table["columns"]
            
            formatted_lines.append(f"\n表名: {table_name}")
            formatted_lines.append("字段:")
            
            for col in columns:
                col_info = f"  - {col['column_name']} ({col['data_type']})"
                if col.get("is_primary_key"):
                    col_info += " [主键]"
                if col.get("is_foreign_key"):
                    col_info += " [外键]"
                if col.get("description"):
                    col_info += f" - {col['description']}"
                
                formatted_lines.append(col_info)
        
        return "\n".join(formatted_lines)
    
    async def _get_few_shot_examples(self, intent_type: str) -> str:
        """获取Few-shot示例"""
        
        examples_map = {
            "select": """
            示例1:
            用户: 查询所有用户的姓名和邮箱
            SQL: SELECT name, email FROM users;
            
            示例2:
            用户: 找出价格大于100的商品
            SQL: SELECT * FROM products WHERE price > 100;
            """,
            
            "aggregation": """
            示例1:
            用户: 统计每个城市的用户数量
            SQL: SELECT city, COUNT(*) as user_count FROM users GROUP BY city;
            
            示例2:
            用户: 计算商品的平均价格
            SQL: SELECT AVG(price) as avg_price FROM products;
            """,
            
            "time_series": """
            示例1:
            用户: 查询最近7天的订单趋势
            SQL: SELECT DATE(created_at) as date, COUNT(*) as order_count 
                 FROM orders 
                 WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                 GROUP BY DATE(created_at) 
                 ORDER BY date;
            """
        }
        
        return examples_map.get(intent_type, examples_map["select"])
    
    def _format_conversation_history(self, messages: List[BaseMessage]) -> str:
        """格式化对话历史"""
        formatted = []
        for msg in messages:
            if isinstance(msg, HumanMessage):
                formatted.append(f"用户: {msg.content}")
            elif isinstance(msg, AIMessage):
                formatted.append(f"助手: {msg.content}")
        
        return "\n".join(formatted[-6:])  # 最近3轮对话
    
    async def explain_sql(self, sql: str) -> str:
        """解释SQL查询"""
        
        explain_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="""
            你是一个SQL查询解释专家。请用简洁明了的中文解释给定的SQL查询语句。

            解释应该包含：
            1. 查询的主要目的
            2. 涉及的表和字段
            3. 过滤条件和逻辑
            4. 排序和限制规则
            5. 性能考量（如果适用）

            用通俗易懂的语言，避免过多技术术语。
            """),
            HumanMessage(content=f"请解释这个SQL查询：\n{sql}")
        ])
        
        response = await self.llm.agenerate([explain_prompt.format_messages()])
        return response.generations[0][0].text
    
    async def suggest_optimizations(self, sql: str) -> List[str]:
        """建议SQL优化方案"""
        
        optimization_prompt = ChatPromptTemplate.from_messages([
            SystemMessage(content="""
            你是一个SQL性能优化专家。请分析给定的SQL查询，提供具体的优化建议。

            请返回JSON格式的优化建议列表，每个建议包含：
            - type: 优化类型 (index, rewrite, structure)
            - description: 优化描述
            - impact: 性能影响 (high, medium, low)
            - sql_example: 优化后的SQL示例（如果适用）
            """),
            HumanMessage(content=f"请为这个SQL查询提供优化建议：\n{sql}")
        ])
        
        response = await self.llm.agenerate([optimization_prompt.format_messages()])
        
        try:
            suggestions = json.loads(response.generations[0][0].text)
            return suggestions
        except json.JSONDecodeError:
            return [{"type": "general", "description": response.generations[0][0].text}]