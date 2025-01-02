/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.persistence.datasource;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.persistence.configuration.DatasourceConfiguration;
import com.alibaba.nacos.persistence.constants.PersistenceConstant;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.DiskUtils;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * 本地数据库服务，即内置的Derby数据库服务
 *
 * @author Nacos
 */
public class LocalDataSourceServiceImpl implements DataSourceService {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDataSourceServiceImpl.class);

    /**
     * Derby启动
     */
    private final String jdbcDriverName = "org.apache.derby.jdbc.EmbeddedDriver";

    /**
     * Derby用户名
     */
    private final String userName = "nacos";

    /**
     * Derby密码
     */
    private final String password = "nacos";

    /**
     * Derby存储路径：data/derby-data
     */
    private final String derbyBaseDir = "data" + File.separator + PersistenceConstant.DERBY_BASE_DIR;

    private final String derbyShutdownErrMsg = "Derby system shutdown.";

    /**
     * 负责执行SQL
     */
    private volatile JdbcTemplate jt;

    /**
     * 负责管理事务
     */
    private volatile TransactionTemplate tjt;
    
    private boolean initialize = false;
    
    private boolean jdbcTemplateInit = false;

    /**
     * 健康状态
     */
    private String healthStatus = "UP";

    /**
     * 数据源类型
     */
    private String dataSourceType = "derby";
    
    @Override
    public synchronized void init() throws Exception {
        /**
         * 如果配置中设置了使用外置数据库，则返回
         */
        if (DatasourceConfiguration.isUseExternalDB()) {
            return;
        }
        /**
         * 如果已经初始化，则跳过初始化；反之开始初始化Derby数据库
         */
        if (!initialize) {
            LOGGER.info("use local db service for init");
            /**
             * 构造数据库url，默认为jdbc:derby:C:\\users\\nacos\\data\\derby-daa;create=true
             */
            final String jdbcUrl = "jdbc:derby:" + Paths.get(EnvUtil.getNacosHome(), derbyBaseDir) + ";create=true";
            /**
             * 执行初始化
             */
            initialize(jdbcUrl);
            /**
             * 更新标志位
             */
            initialize = true;
        }
    }
    
    @Override
    public synchronized void reload() {
        DataSource ds = jt.getDataSource();
        if (ds == null) {
            throw new RuntimeException("datasource is null");
        }
        try {
            execute(ds.getConnection(), "META-INF/derby-schema.sql");
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error(e.getMessage(), e);
            }
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, "load derby-schema.sql error.", e);
        }
    }
    
    public DataSource getDatasource() {
        return jt.getDataSource();
    }
    
    /**
     * Clean and reopen Derby.
     *
     * @throws Exception exception.
     */
    public void cleanAndReopenDerby() throws Exception {
        doDerbyClean();
        final String jdbcUrl = "jdbc:derby:" + Paths.get(EnvUtil.getNacosHome(), derbyBaseDir).toString() + ";create=true";
        initialize(jdbcUrl);
    }
    
    /**
     * Restore derby.
     *
     * @param jdbcUrl  jdbcUrl string value.
     * @param callable callable.
     * @throws Exception exception.
     */
    public void restoreDerby(String jdbcUrl, Callable<Void> callable) throws Exception {
        doDerbyClean();
        callable.call();
        initialize(jdbcUrl);
    }
    
    private void doDerbyClean() throws Exception {
        LOGGER.warn("use local db service for reopenDerby");
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (Exception e) {
            // An error is thrown when the Derby shutdown is executed, which should be ignored
            if (!StringUtils.containsIgnoreCase(e.getMessage(), derbyShutdownErrMsg)) {
                throw e;
            }
        }
        DiskUtils.deleteDirectory(Paths.get(EnvUtil.getNacosHome(), derbyBaseDir).toString());
    }
    
    private synchronized void initialize(String jdbcUrl) {
        /**
         * 初始化数据库连接池
         */
        DataSourcePoolProperties poolProperties = DataSourcePoolProperties.build(EnvUtil.getEnvironment());
        poolProperties.setDriverClassName(jdbcDriverName);
        poolProperties.setJdbcUrl(jdbcUrl);
        poolProperties.setUsername(userName);
        poolProperties.setPassword(password);
        /**
         * 获取数据库连接池
         */
        HikariDataSource ds = poolProperties.getDataSource();
        /**
         * 使用Spring的数据库事务管理器来管理数据库连接池
         */
        DataSourceTransactionManager tm = new DataSourceTransactionManager();
        tm.setDataSource(ds);

        /**
         * 如果JdbcTemplate尚未初始化，则初始化JdbcTemplate和TransactionTemplate；反之
         */
        if (jdbcTemplateInit) {
            jt.setDataSource(ds);
            tjt.setTransactionManager(tm);
        } else {
            /**
             * 初始化JdbcTemplate
             */
            jt = new JdbcTemplate();
            jt.setMaxRows(50000);
            jt.setQueryTimeout(5000);
            jt.setDataSource(ds);
            /**
             * 初始化TransactionTemplate
             */
            tjt = new TransactionTemplate(tm);
            tjt.setTimeout(5000);
            jdbcTemplateInit = true;
        }
        reload();
    }

    /**
     * 默认Master可写
     */
    @Override
    public boolean checkMasterWritable() {
        return true;
    }
    
    @Override
    public JdbcTemplate getJdbcTemplate() {
        return jt;
    }
    
    @Override
    public TransactionTemplate getTransactionTemplate() {
        return tjt;
    }
    
    @Override
    public String getCurrentDbUrl() {
        return "jdbc:derby:" + EnvUtil.getNacosHome() + File.separator + derbyBaseDir + ";create=true";
    }
    
    @Override
    public String getHealth() {
        return healthStatus;
    }
    
    @Override
    public String getDataSourceType() {
        return dataSourceType;
    }
    
    public void setHealthStatus(String healthStatus) {
        this.healthStatus = healthStatus;
    }
    
    /**
     * 默认从${user.home}/nacos/conf/derby-schema.sql读取，如果SQL文件不存在，则从参数读取。
     *
     * @param sqlFile sql.
     * @return sqls.
     * @throws Exception Exception.
     */
    private List<String> loadSql(String sqlFile) throws Exception {
        List<String> sqlList = new ArrayList<>();
        InputStream sqlFileIn = null;
        try {
            File file = new File(EnvUtil.getNacosHome() + File.separator + "conf" + File.separator + "derby-schema.sql");
            if (StringUtils.isBlank(EnvUtil.getNacosHome()) || !file.exists()) {
                ClassLoader classLoader = getClass().getClassLoader();
                URL url = classLoader.getResource(sqlFile);
                sqlFileIn = url.openStream();
            } else {
                sqlFileIn = new FileInputStream(file);
            }
            
            StringBuilder sqlSb = new StringBuilder();
            byte[] buff = new byte[1024];
            int byteRead = 0;
            while ((byteRead = sqlFileIn.read(buff)) != -1) {
                sqlSb.append(new String(buff, 0, byteRead, PersistenceConstant.DEFAULT_ENCODE));
            }
            
            String[] sqlArr = sqlSb.toString().split(";");
            for (int i = 0; i < sqlArr.length; i++) {
                String sql = sqlArr[i].replaceAll("--.*", "").trim();
                if (StringUtils.isNotEmpty(sql)) {
                    sqlList.add(sql);
                }
            }
            return sqlList;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        } finally {
            IoUtils.closeQuietly(sqlFileIn);
        }
    }
    
    /**
     * 从默认地址${user.home}/nacos/conf/derby-schema.sql读取SQL文件，当文件不存在，再从参数中读取。
     * 读取后依次执行。
     *
     * @param conn    connect.
     * @param sqlFile sql.
     * @throws Exception Exception.
     */
    private void execute(Connection conn, String sqlFile) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            List<String> sqlList = loadSql(sqlFile);
            for (String sql : sqlList) {
                try {
                    stmt.execute(sql);
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                }
            }
        }
    }
    
}
