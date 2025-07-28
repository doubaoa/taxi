import React, { useState, useEffect, useRef } from 'react';
import {
  Card,
  Input,
  Button,
  Select,
  Tabs,
  Table,
  Alert,
  Spin,
  Typography,
  Space,
  Tooltip,
  Drawer,
  Switch,
  Slider,
  Tag,
  Progress,
  message,
  Modal,
  Row,
  Col,
} from 'antd';
import {
  SendOutlined,
  DatabaseOutlined,
  HistoryOutlined,
  SettingOutlined,
  DownloadOutlined,
  EyeOutlined,
  ThunderboltOutlined,
  BarChartOutlined,
  CodeOutlined,
  BulbOutlined,
  ShareAltOutlined,
} from '@ant-design/icons';
import { useSelector, useDispatch } from 'react-redux';
import type { RootState } from '../store/store';
import { executeNLQuery, executeSQLQuery, optimizeQuery } from '../store/slices/querySlice';

// 组件
import SQLEditor from '../components/Query/SQLEditor';
import ResultTable from '../components/Query/ResultTable';
import ChartViewer from '../components/Query/ChartViewer';
import QueryHistory from '../components/Query/QueryHistory';
import QueryOptimizer from '../components/Query/QueryOptimizer';
import ExportModal from '../components/Query/ExportModal';

// 钩子
import { useWebSocket } from '../hooks/useWebSocket';
import { useDatabase } from '../hooks/useDatabase';
import { useVisualization } from '../hooks/useVisualization';

const { TextArea } = Input;
const { Option } = Select;
const { TabPane } = Tabs;
const { Title, Text, Paragraph } = Typography;

interface QueryPageProps {}

const QueryPage: React.FC<QueryPageProps> = () => {
  // 状态管理
  const [naturalQuery, setNaturalQuery] = useState('');
  const [sqlQuery, setSqlQuery] = useState('');
  const [selectedDatabase, setSelectedDatabase] = useState<string>('');
  const [activeTab, setActiveTab] = useState('natural');
  const [streamMode, setStreamMode] = useState(false);
  const [showOptimizer, setShowOptimizer] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const [visualizationType, setVisualizationType] = useState<string>('auto');
  const [maxResults, setMaxResults] = useState(1000);

  // Redux状态
  const dispatch = useDispatch();
  const {
    isLoading,
    currentQuery,
    results,
    error,
    executionTime,
    totalRows,
    explanation,
    optimizationSuggestions,
  } = useSelector((state: RootState) => state.query);

  // 自定义钩子
  const { databases, currentDatabase, getDatabases } = useDatabase();
  const { sendMessage, connectionStatus, messages } = useWebSocket();
  const { generateChart, chartData, chartOptions } = useVisualization();

  // Refs
  const queryInputRef = useRef<any>(null);
  const resultTableRef = useRef<any>(null);

  // 初始化
  useEffect(() => {
    getDatabases();
  }, []);

  useEffect(() => {
    if (databases.length > 0 && !selectedDatabase) {
      setSelectedDatabase(databases[0].name);
    }
  }, [databases, selectedDatabase]);

  // 处理自然语言查询
  const handleNaturalQuery = async () => {
    if (!naturalQuery.trim()) {
      message.warning('请输入查询内容');
      return;
    }

    if (!selectedDatabase) {
      message.warning('请选择数据库');
      return;
    }

    try {
      const queryRequest = {
        query: naturalQuery,
        database_name: selectedDatabase,
        stream: streamMode,
        max_results: maxResults,
        visualization_type: visualizationType === 'auto' ? null : visualizationType,
      };

      if (streamMode) {
        // WebSocket流式查询
        sendMessage({
          type: 'nl_query',
          data: queryRequest,
        });
      } else {
        // 普通HTTP查询
        const result = await dispatch(executeNLQuery(queryRequest));
        if (result.meta.requestStatus === 'fulfilled') {
          message.success('查询执行成功');
          // 生成可视化
          if (result.payload.results && result.payload.results.length > 0) {
            generateChart(result.payload.results, visualizationType);
          }
        }
      }
    } catch (err) {
      console.error('Query execution error:', err);
      message.error('查询执行失败');
    }
  };

  // 处理SQL查询
  const handleSQLQuery = async () => {
    if (!sqlQuery.trim()) {
      message.warning('请输入SQL语句');
      return;
    }

    if (!selectedDatabase) {
      message.warning('请选择数据库');
      return;
    }

    try {
      const queryRequest = {
        sql: sqlQuery,
        database_name: selectedDatabase,
        explain_plan: true,
        cache_results: true,
      };

      const result = await dispatch(executeSQLQuery(queryRequest));
      if (result.meta.requestStatus === 'fulfilled') {
        message.success('SQL执行成功');
        // 生成可视化
        if (result.payload.results && result.payload.results.length > 0) {
          generateChart(result.payload.results, visualizationType);
        }
      }
    } catch (err) {
      console.error('SQL execution error:', err);
      message.error('SQL执行失败');
    }
  };

  // 处理查询优化
  const handleOptimizeQuery = async (sql: string) => {
    try {
      const result = await dispatch(
        optimizeQuery({
          sql,
          database_name: selectedDatabase,
          optimization_level: 'medium',
        })
      );

      if (result.meta.requestStatus === 'fulfilled') {
        setSqlQuery(result.payload.optimized_sql);
        message.success('查询优化完成');
      }
    } catch (err) {
      console.error('Query optimization error:', err);
      message.error('查询优化失败');
    }
  };

  // 快捷查询示例
  const quickQueries = [
    '显示所有用户信息',
    '统计每个城市的用户数量',
    '查询最近7天的订单趋势',
    '找出销量最高的商品',
    '计算月度收入统计',
  ];

  // 插入快捷查询
  const insertQuickQuery = (query: string) => {
    setNaturalQuery(query);
    setActiveTab('natural');
  };

  // WebSocket消息处理
  useEffect(() => {
    if (messages.length > 0) {
      const lastMessage = messages[messages.length - 1];
      
      if (lastMessage.type === 'query_progress') {
        // 处理查询进度更新
        console.log('Query progress:', lastMessage.data);
      } else if (lastMessage.type === 'query_result') {
        // 处理查询结果
        console.log('Query result:', lastMessage.data);
        generateChart(lastMessage.data.results, visualizationType);
      } else if (lastMessage.type === 'query_error') {
        // 处理查询错误
        message.error(lastMessage.data.error);
      }
    }
  }, [messages, visualizationType, generateChart]);

  // 键盘快捷键
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.metaKey) {
        switch (event.key) {
          case 'Enter':
            event.preventDefault();
            if (activeTab === 'natural') {
              handleNaturalQuery();
            } else {
              handleSQLQuery();
            }
            break;
          case 'l':
            event.preventDefault();
            queryInputRef.current?.focus();
            break;
          case 'h':
            event.preventDefault();
            setShowHistory(true);
            break;
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [activeTab, naturalQuery, sqlQuery, selectedDatabase]);

  return (
    <div className="query-page">
      {/* 页面头部 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={24}>
          <Card>
            <Row justify="space-between" align="middle">
              <Col>
                <Title level={3} style={{ margin: 0 }}>
                  <DatabaseOutlined /> 智能SQL查询
                </Title>
                <Text type="secondary">
                  使用自然语言或SQL语句查询数据库
                </Text>
              </Col>
              <Col>
                <Space>
                  <Select
                    value={selectedDatabase}
                    onChange={setSelectedDatabase}
                    style={{ width: 200 }}
                    placeholder="选择数据库"
                  >
                    {databases.map((db) => (
                      <Option key={db.name} value={db.name}>
                        <DatabaseOutlined /> {db.display_name || db.name}
                      </Option>
                    ))}
                  </Select>
                  
                  <Tooltip title="查询历史 (Ctrl+H)">
                    <Button
                      icon={<HistoryOutlined />}
                      onClick={() => setShowHistory(true)}
                    />
                  </Tooltip>
                  
                  <Tooltip title="查询优化">
                    <Button
                      icon={<ThunderboltOutlined />}
                      onClick={() => setShowOptimizer(true)}
                    />
                  </Tooltip>
                  
                  <Tooltip title="设置">
                    <Button
                      icon={<SettingOutlined />}
                      onClick={() => {
                        Modal.info({
                          title: '查询设置',
                          content: (
                            <div>
                              <p>流式查询: 
                                <Switch 
                                  checked={streamMode} 
                                  onChange={setStreamMode}
                                  style={{ marginLeft: 8 }}
                                />
                              </p>
                              <p>最大结果数: 
                                <Slider
                                  min={100}
                                  max={10000}
                                  step={100}
                                  value={maxResults}
                                  onChange={setMaxResults}
                                  style={{ width: 200, marginLeft: 8 }}
                                />
                              </p>
                              <p>可视化类型: 
                                <Select
                                  value={visualizationType}
                                  onChange={setVisualizationType}
                                  style={{ width: 120, marginLeft: 8 }}
                                >
                                  <Option value="auto">自动</Option>
                                  <Option value="bar">柱状图</Option>
                                  <Option value="line">折线图</Option>
                                  <Option value="pie">饼图</Option>
                                  <Option value="table">表格</Option>
                                </Select>
                              </p>
                            </div>
                          ),
                        });
                      }}
                    />
                  </Tooltip>
                </Space>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      {/* 连接状态 */}
      {streamMode && (
        <Row style={{ marginBottom: 16 }}>
          <Col span={24}>
            <Alert
              message={
                <Space>
                  <span>实时连接状态:</span>
                  <Tag color={connectionStatus === 'connected' ? 'green' : 'red'}>
                    {connectionStatus === 'connected' ? '已连接' : '未连接'}
                  </Tag>
                </Space>
              }
              type={connectionStatus === 'connected' ? 'success' : 'warning'}
              showIcon
            />
          </Col>
        </Row>
      )}

      {/* 查询输入区域 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={24}>
          <Card>
            <Tabs
              activeKey={activeTab}
              onChange={setActiveTab}
              type="card"
              tabBarExtraContent={
                <Space>
                  <Button
                    type="primary"
                    icon={<SendOutlined />}
                    loading={isLoading}
                    onClick={activeTab === 'natural' ? handleNaturalQuery : handleSQLQuery}
                    disabled={!selectedDatabase}
                  >
                    执行查询 (Ctrl+Enter)
                  </Button>
                </Space>
              }
            >
              <TabPane tab="自然语言查询" key="natural">
                <Space direction="vertical" style={{ width: '100%' }}>
                  <TextArea
                    ref={queryInputRef}
                    placeholder="用自然语言描述您想要查询的内容，例如：显示所有用户信息"
                    value={naturalQuery}
                    onChange={(e) => setNaturalQuery(e.target.value)}
                    autoSize={{ minRows: 3, maxRows: 6 }}
                    onPressEnter={(e) => {
                      if (e.ctrlKey || e.metaKey) {
                        handleNaturalQuery();
                      }
                    }}
                  />
                  
                  {/* 快捷查询 */}
                  <div>
                    <Text strong>快捷查询: </Text>
                    <Space wrap>
                      {quickQueries.map((query, index) => (
                        <Tag
                          key={index}
                          style={{ cursor: 'pointer' }}
                          onClick={() => insertQuickQuery(query)}
                        >
                          {query}
                        </Tag>
                      ))}
                    </Space>
                  </div>
                </Space>
              </TabPane>
              
              <TabPane tab="SQL查询" key="sql">
                <SQLEditor
                  value={sqlQuery}
                  onChange={setSqlQuery}
                  onExecute={handleSQLQuery}
                  database={selectedDatabase}
                  height="200px"
                />
              </TabPane>
            </Tabs>
          </Card>
        </Col>
      </Row>

      {/* 查询信息 */}
      {(currentQuery || error) && (
        <Row style={{ marginBottom: 16 }}>
          <Col span={24}>
            {error ? (
              <Alert
                message="查询执行失败"
                description={error}
                type="error"
                showIcon
                closable
              />
            ) : (
              currentQuery && (
                <Alert
                  message={
                    <Space>
                      <span>执行的SQL:</span>
                      <Tag><CodeOutlined /> {currentQuery.sql}</Tag>
                      {executionTime && (
                        <Tag color="blue">
                          <ThunderboltOutlined /> {executionTime.toFixed(2)}ms
                        </Tag>
                      )}
                      {totalRows !== undefined && (
                        <Tag color="green">
                          共 {totalRows.toLocaleString()} 条记录
                        </Tag>
                      )}
                    </Space>
                  }
                  type="info"
                  showIcon
                  action={
                    <Space>
                      {currentQuery.sql && (
                        <Tooltip title="优化查询">
                          <Button
                            size="small"
                            icon={<BulbOutlined />}
                            onClick={() => handleOptimizeQuery(currentQuery.sql)}
                          />
                        </Tooltip>
                      )}
                      <Tooltip title="分享查询">
                        <Button
                          size="small"
                          icon={<ShareAltOutlined />}
                          onClick={() => {
                            const url = `${window.location.origin}/query?sql=${encodeURIComponent(currentQuery.sql)}`;
                            navigator.clipboard.writeText(url);
                            message.success('查询链接已复制到剪贴板');
                          }}
                        />
                      </Tooltip>
                    </Space>
                  }
                />
              )
            )}
          </Col>
        </Row>
      )}

      {/* 加载状态 */}
      {isLoading && (
        <Row style={{ marginBottom: 16 }}>
          <Col span={24}>
            <Card>
              <div style={{ textAlign: 'center', padding: '40px 0' }}>
                <Spin size="large" />
                <div style={{ marginTop: 16 }}>
                  <Text>正在执行查询...</Text>
                  <Progress
                    percent={30}
                    status="active"
                    showInfo={false}
                    style={{ marginTop: 8 }}
                  />
                </div>
              </div>
            </Card>
          </Col>
        </Row>
      )}

      {/* 结果展示区域 */}
      {results && results.length > 0 && (
        <Row gutter={[16, 16]}>
          <Col span={24}>
            <Card
              title={
                <Space>
                  <BarChartOutlined />
                  查询结果
                  <Tag color="blue">{results.length} 条记录</Tag>
                </Space>
              }
              extra={
                <Space>
                  <Button
                    icon={<EyeOutlined />}
                    onClick={() => {
                      Modal.info({
                        title: '查询解释',
                        content: explanation || '暂无解释信息',
                        width: 600,
                      });
                    }}
                  >
                    查看解释
                  </Button>
                  <Button
                    icon={<DownloadOutlined />}
                    onClick={() => setShowExport(true)}
                  >
                    导出数据
                  </Button>
                </Space>
              }
            >
              <Tabs defaultActiveKey="table">
                <TabPane tab="表格视图" key="table">
                  <ResultTable
                    ref={resultTableRef}
                    data={results}
                    loading={isLoading}
                    pagination={{
                      showSizeChanger: true,
                      showQuickJumper: true,
                      showTotal: (total) => `共 ${total} 条记录`,
                    }}
                  />
                </TabPane>
                
                <TabPane tab="图表视图" key="chart">
                  <ChartViewer
                    data={chartData}
                    options={chartOptions}
                    type={visualizationType}
                  />
                </TabPane>
              </Tabs>
            </Card>
          </Col>
        </Row>
      )}

      {/* 抽屉组件 */}
      <Drawer
        title="查询历史"
        placement="right"
        width={600}
        onClose={() => setShowHistory(false)}
        open={showHistory}
      >
        <QueryHistory
          onSelectQuery={(query) => {
            if (query.natural_query) {
              setNaturalQuery(query.natural_query);
              setActiveTab('natural');
            } else {
              setSqlQuery(query.sql);
              setActiveTab('sql');
            }
            setShowHistory(false);
          }}
        />
      </Drawer>

      <Drawer
        title="查询优化"
        placement="right"
        width={800}
        onClose={() => setShowOptimizer(false)}
        open={showOptimizer}
      >
        <QueryOptimizer
          sql={sqlQuery || currentQuery?.sql || ''}
          onOptimize={handleOptimizeQuery}
          suggestions={optimizationSuggestions}
        />
      </Drawer>

      {/* 导出模态框 */}
      <ExportModal
        visible={showExport}
        onCancel={() => setShowExport(false)}
        data={results}
        filename={`query_result_${Date.now()}`}
      />
    </div>
  );
};

export default QueryPage;