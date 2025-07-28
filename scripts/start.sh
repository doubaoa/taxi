#!/bin/bash

# 智能SQL查询Agent启动脚本
# 支持开发、测试和生产环境的一键部署

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目配置
PROJECT_NAME="intelligent-sql-agent"
BACKEND_IMAGE="sql-agent/backend"
FRONTEND_IMAGE="sql-agent/frontend"

# 显示banner
show_banner() {
    echo -e "${BLUE}"
    echo "=================================="
    echo "   智能SQL查询Agent部署工具"
    echo "=================================="
    echo -e "${NC}"
}

# 显示帮助信息
show_help() {
    echo "用法: $0 [命令] [选项]"
    echo ""
    echo "命令:"
    echo "  dev          启动开发环境"
    echo "  prod         启动生产环境"
    echo "  stop         停止所有服务"
    echo "  clean        清理资源"
    echo "  build        构建镜像"
    echo "  logs         查看日志"
    echo "  status       查看服务状态"
    echo "  backup       备份数据"
    echo "  restore      恢复数据"
    echo "  init         初始化项目"
    echo "  test         运行测试"
    echo ""
    echo "选项:"
    echo "  -h, --help   显示此帮助信息"
    echo "  -v, --verbose 详细输出"
    echo "  -f, --force  强制执行"
    echo ""
    echo "示例:"
    echo "  $0 dev              # 启动开发环境"
    echo "  $0 prod --verbose   # 启动生产环境（详细输出）"
    echo "  $0 logs backend     # 查看后端服务日志"
}

# 检查依赖
check_dependencies() {
    echo -e "${YELLOW}检查系统依赖...${NC}"
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}错误: Docker未安装${NC}"
        echo "请安装Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}错误: Docker Compose未安装${NC}"
        echo "请安装Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    # 检查磁盘空间
    AVAILABLE_SPACE=$(df . | tail -1 | awk '{print $4}')
    REQUIRED_SPACE=5242880  # 5GB in KB
    
    if [ "$AVAILABLE_SPACE" -lt "$REQUIRED_SPACE" ]; then
        echo -e "${RED}警告: 磁盘空间不足，建议至少5GB空闲空间${NC}"
    fi
    
    echo -e "${GREEN}依赖检查通过${NC}"
}

# 创建环境文件
create_env_file() {
    if [ ! -f .env ]; then
        echo -e "${YELLOW}创建环境配置文件...${NC}"
        cat > .env << EOF
# 数据库配置
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=sql_agent

# Redis配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# ClickHouse配置
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# 大模型API配置
OPENAI_API_KEY=your-openai-api-key
ANTHROPIC_API_KEY=your-anthropic-api-key

# 应用配置
SECRET_KEY=$(openssl rand -hex 32)
ENVIRONMENT=development
DEBUG=true

# 安全配置
JWT_SECRET_KEY=$(openssl rand -hex 32)
CORS_ORIGINS=http://localhost:3000,http://localhost:8000

# 监控配置
PROMETHEUS_ENABLED=true
GRAFANA_ADMIN_PASSWORD=admin123
EOF
        echo -e "${GREEN}环境文件已创建: .env${NC}"
        echo -e "${YELLOW}请编辑 .env 文件配置您的API密钥${NC}"
    fi
}

# 创建必要的目录
create_directories() {
    echo -e "${YELLOW}创建项目目录结构...${NC}"
    
    mkdir -p {
        data/{init-scripts,clickhouse-config,uploads,backups},
        logs/{backend,frontend,nginx},
        infrastructure/{nginx/conf.d,ssl,monitoring/grafana/{provisioning,dashboards},logging},
        notebooks,
        cache
    }
    
    echo -e "${GREEN}目录结构创建完成${NC}"
}

# 构建Docker镜像
build_images() {
    echo -e "${YELLOW}构建Docker镜像...${NC}"
    
    if [ "$VERBOSE" = true ]; then
        docker-compose build --no-cache
    else
        docker-compose build --no-cache > /dev/null 2>&1
    fi
    
    echo -e "${GREEN}镜像构建完成${NC}"
}

# 启动开发环境
start_dev() {
    echo -e "${BLUE}启动开发环境...${NC}"
    
    # 启动核心服务
    docker-compose up -d postgres redis
    
    echo -e "${YELLOW}等待数据库启动...${NC}"
    sleep 10
    
    # 运行数据库迁移
    echo -e "${YELLOW}运行数据库迁移...${NC}"
    docker-compose run --rm backend alembic upgrade head
    
    # 启动所有服务
    docker-compose up -d
    
    echo -e "${GREEN}开发环境启动完成！${NC}"
    echo ""
    echo "服务访问地址:"
    echo "  前端应用:    http://localhost:3000"
    echo "  后端API:     http://localhost:8000"
    echo "  API文档:     http://localhost:8000/docs"
    echo "  Flower监控:  http://localhost:5555"
    echo "  Grafana:     http://localhost:3001 (admin/admin123)"
    echo "  Kibana:      http://localhost:5601"
    echo "  Jupyter:     http://localhost:8888 (token: sql-agent-notebook)"
}

# 启动生产环境
start_prod() {
    echo -e "${BLUE}启动生产环境...${NC}"
    
    # 检查环境配置
    if grep -q "your-openai-api-key" .env; then
        echo -e "${RED}错误: 请先配置 .env 文件中的API密钥${NC}"
        exit 1
    fi
    
    # 使用生产配置
    docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
    
    echo -e "${GREEN}生产环境启动完成！${NC}"
}

# 停止服务
stop_services() {
    echo -e "${YELLOW}停止所有服务...${NC}"
    docker-compose down
    echo -e "${GREEN}服务已停止${NC}"
}

# 清理资源
clean_resources() {
    echo -e "${YELLOW}清理项目资源...${NC}"
    
    if [ "$FORCE" = true ]; then
        # 停止并删除容器
        docker-compose down -v --remove-orphans
        
        # 删除镜像
        docker rmi $(docker images "${BACKEND_IMAGE}*" -q) 2>/dev/null || true
        docker rmi $(docker images "${FRONTEND_IMAGE}*" -q) 2>/dev/null || true
        
        # 清理Docker系统
        docker system prune -f
        
        echo -e "${GREEN}资源清理完成${NC}"
    else
        echo -e "${RED}这将删除所有数据和镜像，请使用 --force 参数确认${NC}"
    fi
}

# 查看日志
show_logs() {
    SERVICE=${2:-}
    
    if [ -n "$SERVICE" ]; then
        echo -e "${BLUE}查看 $SERVICE 服务日志...${NC}"
        docker-compose logs -f "$SERVICE"
    else
        echo -e "${BLUE}查看所有服务日志...${NC}"
        docker-compose logs -f
    fi
}

# 查看状态
show_status() {
    echo -e "${BLUE}服务状态:${NC}"
    docker-compose ps
    
    echo ""
    echo -e "${BLUE}资源使用情况:${NC}"
    docker stats --no-stream
}

# 备份数据
backup_data() {
    BACKUP_DIR="./data/backups"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    
    echo -e "${YELLOW}开始数据备份...${NC}"
    
    # 备份PostgreSQL
    docker-compose exec -T postgres pg_dump -U postgres sql_agent > "$BACKUP_DIR/postgres_$TIMESTAMP.sql"
    
    # 备份Redis
    docker-compose exec -T redis redis-cli BGSAVE
    docker cp sql-agent-redis:/data/dump.rdb "$BACKUP_DIR/redis_$TIMESTAMP.rdb"
    
    # 压缩备份
    tar -czf "$BACKUP_DIR/backup_$TIMESTAMP.tar.gz" -C "$BACKUP_DIR" \
        "postgres_$TIMESTAMP.sql" "redis_$TIMESTAMP.rdb"
    
    # 清理临时文件
    rm "$BACKUP_DIR/postgres_$TIMESTAMP.sql" "$BACKUP_DIR/redis_$TIMESTAMP.rdb"
    
    echo -e "${GREEN}备份完成: backup_$TIMESTAMP.tar.gz${NC}"
}

# 恢复数据
restore_data() {
    BACKUP_FILE=$2
    
    if [ -z "$BACKUP_FILE" ]; then
        echo -e "${RED}请指定备份文件${NC}"
        echo "用法: $0 restore <backup_file>"
        exit 1
    fi
    
    if [ ! -f "$BACKUP_FILE" ]; then
        echo -e "${RED}备份文件不存在: $BACKUP_FILE${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}开始数据恢复...${NC}"
    
    # 解压备份
    TEMP_DIR=$(mktemp -d)
    tar -xzf "$BACKUP_FILE" -C "$TEMP_DIR"
    
    # 恢复PostgreSQL
    docker-compose exec -T postgres psql -U postgres -c "DROP DATABASE IF EXISTS sql_agent;"
    docker-compose exec -T postgres psql -U postgres -c "CREATE DATABASE sql_agent;"
    docker-compose exec -T postgres psql -U postgres sql_agent < "$TEMP_DIR/"postgres_*.sql
    
    # 恢复Redis
    docker cp "$TEMP_DIR/"redis_*.rdb sql-agent-redis:/data/dump.rdb
    docker-compose restart redis
    
    # 清理临时文件
    rm -rf "$TEMP_DIR"
    
    echo -e "${GREEN}数据恢复完成${NC}"
}

# 初始化项目
init_project() {
    echo -e "${BLUE}初始化项目...${NC}"
    
    check_dependencies
    create_env_file
    create_directories
    
    # 创建初始化SQL脚本
    cat > data/init-scripts/01-init.sql << 'EOF'
-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 创建示例数据表
CREATE TABLE IF NOT EXISTS sample_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    city VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入示例数据
INSERT INTO sample_users (name, email, city) VALUES
    ('张三', 'zhangsan@example.com', '北京'),
    ('李四', 'lisi@example.com', '上海'),
    ('王五', 'wangwu@example.com', '广州'),
    ('赵六', 'zhaoliu@example.com', '深圳'),
    ('钱七', 'qianqi@example.com', '杭州')
ON CONFLICT DO NOTHING;
EOF
    
    echo -e "${GREEN}项目初始化完成${NC}"
    echo -e "${YELLOW}请编辑 .env 文件配置API密钥，然后运行: $0 dev${NC}"
}

# 运行测试
run_tests() {
    echo -e "${BLUE}运行项目测试...${NC}"
    
    # 后端测试
    echo -e "${YELLOW}运行后端测试...${NC}"
    docker-compose run --rm backend pytest tests/ -v
    
    # 前端测试
    echo -e "${YELLOW}运行前端测试...${NC}"
    docker-compose run --rm frontend npm test -- --coverage --watchAll=false
    
    echo -e "${GREEN}测试完成${NC}"
}

# 健康检查
health_check() {
    echo -e "${BLUE}执行健康检查...${NC}"
    
    # 检查服务状态
    SERVICES=("backend" "frontend" "postgres" "redis" "clickhouse")
    
    for service in "${SERVICES[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            echo -e "${GREEN}✓ $service 运行正常${NC}"
        else
            echo -e "${RED}✗ $service 运行异常${NC}"
        fi
    done
    
    # 检查API健康状态
    if curl -s http://localhost:8000/health | grep -q "healthy"; then
        echo -e "${GREEN}✓ API健康检查通过${NC}"
    else
        echo -e "${RED}✗ API健康检查失败${NC}"
    fi
}

# 解析命令行参数
VERBOSE=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        *)
            COMMAND=$1
            shift
            ;;
    esac
done

# 主程序
main() {
    show_banner
    
    case $COMMAND in
        dev)
            check_dependencies
            create_env_file
            create_directories
            build_images
            start_dev
            ;;
        prod)
            check_dependencies
            start_prod
            ;;
        stop)
            stop_services
            ;;
        clean)
            clean_resources
            ;;
        build)
            build_images
            ;;
        logs)
            show_logs "$@"
            ;;
        status)
            show_status
            ;;
        backup)
            backup_data
            ;;
        restore)
            restore_data "$@"
            ;;
        init)
            init_project
            ;;
        test)
            run_tests
            ;;
        health)
            health_check
            ;;
        *)
            if [ -z "$COMMAND" ]; then
                show_help
            else
                echo -e "${RED}未知命令: $COMMAND${NC}"
                show_help
                exit 1
            fi
            ;;
    esac
}

# 执行主程序
main "$@"