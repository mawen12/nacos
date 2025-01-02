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

package com.alibaba.nacos.persistence.repository.embedded.operate;

import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder;
import com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest;
import org.springframework.jdbc.core.RowMapper;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * 数据查询操作接口
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public interface DatabaseOperate {
    
    /**
     * 根据SQL查询一条记录
     *
     * @param sql sqk text
     * @param cls target type
     * @param <R> return type
     * @return query result
     */
    <R> R queryOne(String sql, Class<R> cls);
    
    /**
     * 根据SQL和参数查询一条记录
     *
     * @param sql  sqk text
     * @param args sql parameters
     * @param cls  target type
     * @param <R>  return type
     * @return query result
     */
    <R> R queryOne(String sql, Object[] args, Class<R> cls);
    
    /**
     * 使用SQL和参数查询一条记录
     *
     * @param sql    sqk text
     * @param args   sql parameters
     * @param mapper Database query result converter
     * @param <R>    return type
     * @return query result
     */
    <R> R queryOne(String sql, Object[] args, RowMapper<R> mapper);
    
    /**
     * 使用SQL和参数查询多条记录
     *
     * @param sql    sqk text
     * @param args   sql parameters
     * @param mapper Database query result converter
     * @param <R>    return type
     * @return query result
     */
    <R> List<R> queryMany(String sql, Object[] args, RowMapper<R> mapper);
    
    /**
     * 使用SQL和参数查询多条记录
     *
     * @param sql    sqk text
     * @param args   sql parameters
     * @param rClass target type
     * @param <R>    return type
     * @return query result
     */
    <R> List<R> queryMany(String sql, Object[] args, Class<R> rClass);
    
    /**
     * 使用SQL和参数查询多条记录
     *
     * @param sql  sqk text
     * @param args sql parameters
     * @return query result
     */
    List<Map<String, Object>> queryMany(String sql, Object[] args);
    
    /**
     * 数据更新操作
     *
     * @param modifyRequests {@link List}
     * @param consumer       {@link BiConsumer}
     * @return is success
     */
    Boolean update(List<ModifyRequest> modifyRequests, BiConsumer<Boolean, Throwable> consumer);
    
    /**
     * 数据更新操作
     *
     * @param modifyRequests {@link List}
     * @return is success
     */
    default Boolean update(List<ModifyRequest> modifyRequests) {
        return update(modifyRequests, null);
    }
    
    /**
     * 数据导入，用于从外部数据源导入到内置数据库中
     *
     * @param file {@link File}
     * @return {@link CompletableFuture}
     */
    CompletableFuture<RestResult<String>> dataImport(File file);
    
    /**
     * 从{@link EmbeddedStorageContextHolder#SQL_CONTEXT}中获取要执行的SQL，并在执行后清除
     *
     * @return is success
     */
    default Boolean blockUpdate() {
        return blockUpdate(null);
    }
    
    /**
     * 从{@link EmbeddedStorageContextHolder#SQL_CONTEXT}获取要执行的SQL，并在执行后清除
     *
     * @author klw(213539@qq.com)
     * 2020/8/24 18:16
     * @param consumer the consumer
     * @return java.lang.Boolean
     */
    default Boolean blockUpdate(BiConsumer<Boolean, Throwable> consumer) {
        try {
            /**
             * 从线程本地变量读取要执行的{@link ModifyRequest}
             */
            return update(EmbeddedStorageContextHolder.getCurrentSqlContext(), consumer);
        } finally {
            /**
             * 清楚本地的SQL
             */
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }
    
    /**
     * data modify transaction The SqlContext to be executed in the current thread will be executed and automatically
     * cleared.
     *
     *
     * @return is success
     */
    default CompletableFuture<Boolean> futureUpdate() {
        try {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            update(EmbeddedStorageContextHolder.getCurrentSqlContext(), (o, throwable) -> {
                if (Objects.nonNull(throwable)) {
                    future.completeExceptionally(throwable);
                    return;
                }
                future.complete(o);
            });
            return future;
        } finally {
            EmbeddedStorageContextHolder.cleanAllContext();
        }
    }
    
}
