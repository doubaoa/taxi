"""
应用配置管理
"""
import secrets
from typing import List, Optional, Union
from pydantic import AnyHttpUrl, BaseSettings, validator


class Settings(BaseSettings):
    """应用配置类"""
    
    # 基础配置
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8天
    SERVER_NAME: str = "SQL Agent API"
    SERVER_HOST: AnyHttpUrl = "http://localhost"
    
    # CORS配置
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    
    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)
    
    # 数据库配置
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password"
    POSTGRES_DB: str = "sql_agent"
    POSTGRES_PORT: int = 5432
    
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # Redis配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = None
    
    @property
    def REDIS_URL(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    # ClickHouse配置
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"
    
    # Spark配置
    SPARK_MASTER: str = "local[*]"
    SPARK_APP_NAME: str = "SQL-Agent"
    JAVA_HOME: str = "/usr/lib/jvm/java-8-openjdk-amd64"
    
    # 大模型API配置
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_API_BASE: str = "https://api.openai.com/v1"
    OPENAI_MODEL: str = "gpt-4-turbo-preview"
    
    ANTHROPIC_API_KEY: Optional[str] = None
    ANTHROPIC_MODEL: str = "claude-3-sonnet-20240229"
    
    # 本地模型配置
    LOCAL_MODEL_PATH: Optional[str] = None
    LOCAL_MODEL_NAME: str = "llama2-7b-chat"
    
    # 向量数据库配置
    CHROMA_PERSIST_DIRECTORY: str = "./data/chroma"
    EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    
    # Celery配置
    CELERY_BROKER_URL: str = REDIS_URL
    CELERY_RESULT_BACKEND: str = REDIS_URL
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_RESULT_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"
    CELERY_ENABLE_UTC: bool = True
    
    # 监控配置
    PROMETHEUS_ENABLED: bool = True
    PROMETHEUS_PORT: int = 8001
    
    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    # 安全配置
    RATE_LIMIT_PER_MINUTE: int = 60
    MAX_QUERY_COMPLEXITY: int = 100
    ALLOWED_SQL_KEYWORDS: List[str] = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", 
        "HAVING", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
        "UNION", "DISTINCT", "LIMIT", "OFFSET"
    ]
    FORBIDDEN_SQL_KEYWORDS: List[str] = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", 
        "CREATE", "TRUNCATE", "REPLACE"
    ]
    
    # 缓存配置
    QUERY_CACHE_TTL: int = 3600  # 1小时
    RESULT_CACHE_TTL: int = 1800  # 30分钟
    METADATA_CACHE_TTL: int = 86400  # 24小时
    
    # 文件上传配置
    MAX_FILE_SIZE: int = 100 * 1024 * 1024  # 100MB
    ALLOWED_FILE_TYPES: List[str] = [".csv", ".xlsx", ".json", ".parquet"]
    UPLOAD_DIRECTORY: str = "./data/uploads"
    
    # 可视化配置
    MAX_CHART_POINTS: int = 10000
    DEFAULT_CHART_WIDTH: int = 800
    DEFAULT_CHART_HEIGHT: int = 600
    
    # 环境变量
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    TESTING: bool = False
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()