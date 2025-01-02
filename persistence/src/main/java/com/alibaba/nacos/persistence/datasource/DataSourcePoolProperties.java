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

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.util.concurrent.TimeUnit;

/**
 * 数据源连接池配置
 * Nacos Server使用HikariCP作为数据库连接池，因此内部返回数据源为{@link com.zaxxer.hikari.HikariDataSource}
 *
 * @author xiweng.yy
 */
public class DataSourcePoolProperties {

    /**
     * 默认连接超时时间3s
     */
    public static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(3L);

    /**
     * 默认校验超时时间10s
     */
    public static final long DEFAULT_VALIDATION_TIMEOUT = TimeUnit.SECONDS.toMillis(10L);

    /**
     * 连接默认超时时间10s
     */
    public static final long DEFAULT_IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(10L);

    /**
     * 默认连接池中最大的连接数
     */
    public static final int DEFAULT_MAX_POOL_SIZE = 20;

    /**
     * 默认连接池中最小的连接数
     */
    public static final int DEFAULT_MINIMUM_IDLE = 2;

    private final HikariDataSource dataSource;

    private DataSourcePoolProperties() {
        /**
         * 初始化数据源
         */
        dataSource = new HikariDataSource();
        dataSource.setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
        dataSource.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
        dataSource.setValidationTimeout(DEFAULT_VALIDATION_TIMEOUT);
        dataSource.setMaximumPoolSize(DEFAULT_MAX_POOL_SIZE);
        dataSource.setMinimumIdle(DEFAULT_MINIMUM_IDLE);
    }

    /**
     * Build new Hikari config.
     *
     * @return new hikari config
     */
    public static DataSourcePoolProperties build(Environment environment) {
        DataSourcePoolProperties result = new DataSourcePoolProperties();
        // what to do?
        Binder.get(environment).bind("db.pool.config", Bindable.ofInstance(result.getDataSource()));
        return result;
    }

    public void setDriverClassName(final String driverClassName) {
        dataSource.setDriverClassName(driverClassName);
    }

    public void setJdbcUrl(final String jdbcUrl) {
        dataSource.setJdbcUrl(jdbcUrl);
    }

    public void setUsername(final String username) {
        dataSource.setUsername(username);
    }

    public void setPassword(final String password) {
        dataSource.setPassword(password);
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }
}
