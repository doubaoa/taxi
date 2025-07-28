# 智能SQL查询Agent部署指南

## 概述

本文档提供了智能SQL查询Agent系统的完整部署指南，包括开发环境、测试环境和生产环境的部署方案。

## 系统要求

### 最小系统要求
- **CPU**: 4核心 (推荐8核心)
- **内存**: 8GB RAM (推荐16GB)
- **存储**: 50GB 可用磁盘空间
- **网络**: 稳定的互联网连接

### 软件依赖
- Docker 20.10+
- Docker Compose 2.0+
- Kubernetes 1.24+ (生产环境)
- kubectl CLI工具
- Helm 3.8+ (可选)

## 部署架构

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│      前端服务        │    │      后端服务        │    │     数据库服务       │
│   (React + Nginx)   │◄──►│  (FastAPI + Python) │◄──►│ (PostgreSQL + Redis)│
│                     │    │                     │    │                     │
│ - Web界面           │    │ - API服务           │    │ - 元数据存储         │
│ - 用户交互           │    │ - Agent逻辑         │    │ - 缓存服务           │
│ - 结果展示           │    │ - 查询处理           │    │ - 会话管理           │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                      │
                           ┌─────────────────────┐
                           │     分析数据库       │
                           │   (ClickHouse)      │
                           │                     │
                           │ - OLAP查询          │
                           │ - 大数据分析         │
                           │ - 时序数据           │
                           └─────────────────────┘
```

## 快速开始

### 1. 开发环境部署

#### 1.1 克隆项目
```bash
git clone https://github.com/your-org/intelligent-sql-agent.git
cd intelligent-sql-agent
```

#### 1.2 环境配置
```bash
# 复制环境变量模板
cp .env.example .env

# 编辑环境变量
vim .env
```

必需的环境变量配置：
```bash
# 数据库配置
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DB=sql_agent

# Redis配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# 大模型API配置
OPENAI_API_KEY=sk-your-openai-key
ANTHROPIC_API_KEY=your-anthropic-key

# 应用配置
SECRET_KEY=your-secret-key
ENVIRONMENT=development
DEBUG=true
```

#### 1.3 Docker Compose启动
```bash
# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

#### 1.4 初始化数据库
```bash
# 运行数据库迁移
docker-compose exec backend alembic upgrade head

# 创建初始用户
docker-compose exec backend python scripts/create_admin_user.py
```

#### 1.5 验证部署
访问 http://localhost:3000 验证前端服务
访问 http://localhost:8000/docs 验证后端API文档

### 2. 生产环境部署

#### 2.1 Kubernetes集群准备

确保Kubernetes集群已准备就绪：
```bash
# 检查集群状态
kubectl cluster-info

# 检查节点状态
kubectl get nodes

# 创建命名空间
kubectl apply -f infrastructure/kubernetes/namespace.yaml
```

#### 2.2 配置密钥和ConfigMap

##### 创建密钥
```bash
# 创建API密钥
kubectl create secret generic sql-agent-secrets \
  --from-literal=OPENAI_API_KEY="sk-your-openai-key" \
  --from-literal=ANTHROPIC_API_KEY="your-anthropic-key" \
  --from-literal=SECRET_KEY="your-secret-key" \
  --from-literal=POSTGRES_PASSWORD="your-db-password" \
  -n sql-agent
```

##### 应用ConfigMap
```bash
kubectl apply -f infrastructure/kubernetes/configmap.yaml
```

#### 2.3 存储配置

##### 创建存储类（如果需要）
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

```bash
kubectl apply -f infrastructure/kubernetes/storage-class.yaml
```

#### 2.4 部署应用
```bash
# 部署所有组件
kubectl apply -f infrastructure/kubernetes/deployment.yaml

# 检查部署状态
kubectl get pods -n sql-agent
kubectl get services -n sql-agent
```

#### 2.5 配置Ingress和SSL

##### 安装Cert-Manager（如果未安装）
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
```

##### 创建ClusterIssuer
```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: admin@yourdomain.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
    - http01:
        ingress:
          class: nginx
```

```bash
kubectl apply -f infrastructure/kubernetes/cluster-issuer.yaml
```

#### 2.6 监控和日志

##### 部署Prometheus监控
```bash
# 添加Prometheus Helm仓库
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# 安装Prometheus Operator
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --values infrastructure/monitoring/prometheus-values.yaml
```

##### 部署ELK日志系统
```bash
# 添加Elastic Helm仓库
helm repo add elastic https://helm.elastic.co
helm repo update

# 安装Elasticsearch
helm install elasticsearch elastic/elasticsearch \
  --namespace logging \
  --create-namespace \
  --values infrastructure/logging/elasticsearch-values.yaml

# 安装Kibana
helm install kibana elastic/kibana \
  --namespace logging \
  --values infrastructure/logging/kibana-values.yaml

# 安装Filebeat
helm install filebeat elastic/filebeat \
  --namespace logging \
  --values infrastructure/logging/filebeat-values.yaml
```

## 高级配置

### 1. 自动扩缩容配置

#### HPA配置示例
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sql-agent-backend-hpa
  namespace: sql-agent
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sql-agent-backend
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 10
        periodSeconds: 60
```

#### VPA配置（垂直扩缩容）
```yaml
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: sql-agent-backend-vpa
  namespace: sql-agent
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sql-agent-backend
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
    - containerName: backend
      minAllowed:
        cpu: 100m
        memory: 128Mi
      maxAllowed:
        cpu: 2000m
        memory: 4Gi
```

### 2. 多环境配置

#### 开发环境配置
```yaml
# dev-values.yaml
replicaCount: 1
image:
  tag: "dev"
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

#### 测试环境配置
```yaml
# staging-values.yaml
replicaCount: 2
image:
  tag: "staging"
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "1Gi"
    cpu: "750m"
```

#### 生产环境配置
```yaml
# prod-values.yaml
replicaCount: 3
image:
  tag: "v1.0.0"
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

### 3. 安全配置

#### Pod Security Standards
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: sql-agent
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

#### Network Policies
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sql-agent-network-policy
  namespace: sql-agent
spec:
  podSelector:
    matchLabels:
      app: sql-agent-backend
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: sql-agent-frontend
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - podSelector:
        matchLabels:
          app: postgres
    ports:
    - protocol: TCP
      port: 5432
  - to:
    - podSelector:
        matchLabels:
          app: redis
    ports:
    - protocol: TCP
      port: 6379
```

## 运维指南

### 1. 健康检查

#### 服务健康检查脚本
```bash
#!/bin/bash
# health-check.sh

NAMESPACE="sql-agent"

echo "检查Pod状态..."
kubectl get pods -n $NAMESPACE

echo "检查服务状态..."
kubectl get services -n $NAMESPACE

echo "检查Ingress状态..."
kubectl get ingress -n $NAMESPACE

echo "检查后端API健康状态..."
kubectl port-forward svc/sql-agent-backend 8000:8000 -n $NAMESPACE &
PF_PID=$!
sleep 5

HEALTH_RESPONSE=$(curl -s http://localhost:8000/health)
if [[ $HEALTH_RESPONSE == *"healthy"* ]]; then
    echo "✅ 后端服务健康"
else
    echo "❌ 后端服务异常"
fi

kill $PF_PID
```

### 2. 备份和恢复

#### 数据库备份脚本
```bash
#!/bin/bash
# backup-database.sh

NAMESPACE="sql-agent"
BACKUP_DIR="/backups"
DATE=$(date +%Y%m%d_%H%M%S)

# 创建数据库备份
kubectl exec -n $NAMESPACE deployment/postgres -- pg_dump -U postgres sql_agent > $BACKUP_DIR/sql_agent_$DATE.sql

# 压缩备份文件
gzip $BACKUP_DIR/sql_agent_$DATE.sql

# 清理旧备份（保留7天）
find $BACKUP_DIR -name "sql_agent_*.sql.gz" -mtime +7 -delete

echo "数据库备份完成: sql_agent_$DATE.sql.gz"
```

#### 数据库恢复脚本
```bash
#!/bin/bash
# restore-database.sh

NAMESPACE="sql-agent"
BACKUP_FILE=$1

if [ -z "$BACKUP_FILE" ]; then
    echo "用法: $0 <backup_file>"
    exit 1
fi

# 解压备份文件
gunzip -c $BACKUP_FILE > /tmp/restore.sql

# 恢复数据库
kubectl exec -i -n $NAMESPACE deployment/postgres -- psql -U postgres sql_agent < /tmp/restore.sql

# 清理临时文件
rm /tmp/restore.sql

echo "数据库恢复完成"
```

### 3. 日志管理

#### 日志收集配置
```yaml
apiVersion: logging.coreos.com/v1
kind: ClusterLogForwarder
metadata:
  name: sql-agent-logs
  namespace: openshift-logging
spec:
  outputs:
  - name: elasticsearch-sql-agent
    type: "elasticsearch"
    url: http://elasticsearch.logging.svc.cluster.local:9200
    elasticsearch:
      index: sql-agent-logs
  pipelines:
  - name: sql-agent-pipeline
    inputRefs:
    - application
    filterRefs:
    - sql-agent-filter
    outputRefs:
    - elasticsearch-sql-agent
```

### 4. 性能优化

#### 资源配置优化
```yaml
# 后端服务优化配置
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "2000m"

# JVM优化（如果使用Java组件）
env:
- name: JAVA_OPTS
  value: "-Xms1g -Xmx3g -XX:+UseG1GC -XX:+UseStringDeduplication"

# 数据库连接池优化
- name: DB_POOL_SIZE
  value: "20"
- name: DB_MAX_OVERFLOW
  value: "30"
```

## 故障排除

### 1. 常见问题

#### Pod启动失败
```bash
# 查看Pod事件
kubectl describe pod <pod-name> -n sql-agent

# 查看Pod日志
kubectl logs <pod-name> -n sql-agent --previous

# 检查资源限制
kubectl top pods -n sql-agent
```

#### 服务连接问题
```bash
# 测试服务连接
kubectl port-forward svc/sql-agent-backend 8000:8000 -n sql-agent

# 检查DNS解析
kubectl exec -it <pod-name> -n sql-agent -- nslookup postgres

# 测试网络连通性
kubectl exec -it <pod-name> -n sql-agent -- telnet postgres 5432
```

#### 存储问题
```bash
# 检查PVC状态
kubectl get pvc -n sql-agent

# 查看存储类
kubectl get storageclass

# 检查磁盘使用率
kubectl exec -it <pod-name> -n sql-agent -- df -h
```

### 2. 性能问题诊断

#### CPU和内存使用率
```bash
# 查看资源使用情况
kubectl top pods -n sql-agent
kubectl top nodes

# 设置资源监控
kubectl apply -f infrastructure/monitoring/resource-monitor.yaml
```

#### 数据库性能分析
```sql
-- 查看慢查询
SELECT query, mean_time, calls, total_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- 查看数据库连接数
SELECT count(*) FROM pg_stat_activity;

-- 查看锁等待
SELECT * FROM pg_locks WHERE NOT granted;
```

## 扩展指南

### 1. 添加新的数据源

#### 配置新数据库连接
```python
# backend/app/core/database_connections.py
DATABASE_CONFIGS = {
    'mysql_db': {
        'driver': 'mysql+pymysql',
        'host': 'mysql.example.com',
        'port': 3306,
        'database': 'production',
        'username': 'readonly_user',
        'password': 'secure_password'
    },
    'snowflake_db': {
        'driver': 'snowflake',
        'account': 'your_account',
        'warehouse': 'COMPUTE_WH',
        'database': 'ANALYTICS',
        'schema': 'PUBLIC'
    }
}
```

### 2. 集成新的大模型

#### 添加模型配置
```python
# backend/app/core/llm_providers.py
class LocalLLMProvider(BaseLLMProvider):
    def __init__(self, model_path: str):
        self.model = LlamaCpp(
            model_path=model_path,
            temperature=0.1,
            max_tokens=2048,
            n_ctx=4096
        )
    
    async def generate_sql(self, prompt: str) -> str:
        response = await self.model.agenerate([prompt])
        return response.generations[0][0].text
```

## 结论

本部署指南涵盖了智能SQL查询Agent系统的完整部署流程，从开发环境到生产环境的各种场景。遵循本指南可以确保系统的稳定运行和最佳性能。

如有问题，请参考故障排除章节或联系技术支持团队。