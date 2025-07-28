import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme, Layout, message } from 'antd';
import { Provider } from 'react-redux';
import { store } from './store/store';
import { AuthProvider } from './contexts/AuthContext';
import { WebSocketProvider } from './contexts/WebSocketContext';

// 页面组件
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import QueryPage from './pages/QueryPage';
import HistoryPage from './pages/HistoryPage';
import SettingsPage from './pages/SettingsPage';

// 布局组件
import AppHeader from './components/Layout/AppHeader';
import AppSidebar from './components/Layout/AppSidebar';
import AppFooter from './components/Layout/AppFooter';

// 工具函数
import { useAuth } from './hooks/useAuth';
import { useTheme } from './hooks/useTheme';

const { Header, Sider, Content, Footer } = Layout;

// 受保护的路由组件
const ProtectedRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, loading } = useAuth();

  if (loading) {
    return <div>Loading...</div>;
  }

  return isAuthenticated ? <>{children}</> : <Navigate to="/login" />;
};

// 主应用布局
const AppLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [collapsed, setCollapsed] = useState(false);
  const { isDarkMode } = useTheme();

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider 
        collapsible 
        collapsed={collapsed} 
        onCollapse={(value) => setCollapsed(value)}
        theme={isDarkMode ? 'dark' : 'light'}
      >
        <AppSidebar collapsed={collapsed} />
      </Sider>
      
      <Layout>
        <Header style={{ padding: 0, background: isDarkMode ? '#001529' : '#fff' }}>
          <AppHeader collapsed={collapsed} setCollapsed={setCollapsed} />
        </Header>
        
        <Content style={{ margin: '24px 16px', padding: 24, minHeight: 280 }}>
          {children}
        </Content>
        
        <Footer style={{ textAlign: 'center' }}>
          <AppFooter />
        </Footer>
      </Layout>
    </Layout>
  );
};

// 主应用组件
const App: React.FC = () => {
  const { isDarkMode } = useTheme();

  useEffect(() => {
    // 全局错误处理
    const handleError = (event: ErrorEvent) => {
      console.error('Global error:', event.error);
      message.error('发生了一个错误，请刷新页面重试');
    };

    const handleUnhandledRejection = (event: PromiseRejectionEvent) => {
      console.error('Unhandled promise rejection:', event.reason);
      message.error('网络请求失败，请检查网络连接');
    };

    window.addEventListener('error', handleError);
    window.addEventListener('unhandledrejection', handleUnhandledRejection);

    return () => {
      window.removeEventListener('error', handleError);
      window.removeEventListener('unhandledrejection', handleUnhandledRejection);
    };
  }, []);

  return (
    <Provider store={store}>
      <ConfigProvider
        theme={{
          algorithm: isDarkMode ? theme.darkAlgorithm : theme.defaultAlgorithm,
          token: {
            colorPrimary: '#1890ff',
            borderRadius: 6,
            wireframe: false,
          },
        }}
      >
        <AuthProvider>
          <WebSocketProvider>
            <Router>
              <div className="App">
                <Routes>
                  {/* 登录页面 */}
                  <Route path="/login" element={<LoginPage />} />
                  
                  {/* 受保护的路由 */}
                  <Route
                    path="/"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <Navigate to="/dashboard" replace />
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                  
                  <Route
                    path="/dashboard"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <DashboardPage />
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                  
                  <Route
                    path="/query"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <QueryPage />
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                  
                  <Route
                    path="/history"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <HistoryPage />
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                  
                  <Route
                    path="/settings"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <SettingsPage />
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                  
                  {/* 404页面 */}
                  <Route
                    path="*"
                    element={
                      <ProtectedRoute>
                        <AppLayout>
                          <div style={{ textAlign: 'center', padding: '50px' }}>
                            <h1>404 - 页面未找到</h1>
                            <p>您访问的页面不存在</p>
                          </div>
                        </AppLayout>
                      </ProtectedRoute>
                    }
                  />
                </Routes>
              </div>
            </Router>
          </WebSocketProvider>
        </AuthProvider>
      </ConfigProvider>
    </Provider>
  );
};

export default App;