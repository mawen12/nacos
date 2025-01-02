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

import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.persistence.repository.embedded.EmbeddedStorageContextHolder;
import com.alibaba.nacos.persistence.repository.embedded.sql.ModifyRequest;
import com.alibaba.nacos.persistence.utils.DerbyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

/**
 * The Derby database basic operation.
 *
 * 支持基本的数据查询操作接口
 *
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@SuppressWarnings("PMD.AbstractMethodOrInterfaceMethodMustUseJavadocRule")
public interface BaseDatabaseOperate extends DatabaseOperate {
    
    Logger LOGGER = LoggerFactory.getLogger(BaseDatabaseOperate.class);
    
    /**
     * 使用{@link JdbcTemplate}执行SQL，获取一条记录并转换为指定类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param cls          target type
     * @param <R>          target type
     * @return R
     */
    default <R> R queryOne(JdbcTemplate jdbcTemplate, String sql, Class<R> cls) {
        try {
            return jdbcTemplate.queryForObject(sql, cls);
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            /**
             * 无法获取连接时抛出异常
             */
            LOGGER.error("[db-error] can't get connection : {}", ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        } catch (DataAccessException e) {
            /**
             * 数据访问异常
             */
            LOGGER.error("[db-error] DataAccessException : {}", ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * query one result by sql and args then convert result to target type.
     * 使用{@link JdbcTemplate}执行SQL，获取一条记录并转换为指定类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param args         args
     * @param cls          target type
     * @param <R>          target type
     * @return R
     */
    default <R> R queryOne(JdbcTemplate jdbcTemplate, String sql, Object[] args, Class<R> cls) {
        try {
            return jdbcTemplate.queryForObject(sql, args, cls);
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LOGGER.error("[db-error] {}", e.toString());
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", sql, args,
                    ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * 使用{@link JdbcTemplate}执行SQL，获取一条记录并使用{@link RowMapper}转换为指定类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param args         args
     * @param mapper       {@link RowMapper}
     * @param <R>          target type
     * @return R
     */
    default <R> R queryOne(JdbcTemplate jdbcTemplate, String sql, Object[] args, RowMapper<R> mapper) {
        try {
            return jdbcTemplate.queryForObject(sql, args, mapper);
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LOGGER.error("[db-error] {}", e.toString());
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", sql, args,
                    ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * 使用{@link JdbcTemplate}执行SQL，获取多条记录并使用{@link RowMapper}转换为指定类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param args         args
     * @param mapper       {@link RowMapper}
     * @param <R>          target type
     * @return result list
     */
    default <R> List<R> queryMany(JdbcTemplate jdbcTemplate, String sql, Object[] args, RowMapper<R> mapper) {
        try {
            return jdbcTemplate.query(sql, args, mapper);
        } catch (CannotGetJdbcConnectionException e) {
            LOGGER.error("[db-error] {}", e.toString());
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", sql, args,
                    ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * 使用{@link JdbcTemplate}执行SQL，获取多条记录并转换为指定类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param args         args
     * @param rClass       target type class
     * @param <R>          target type
     * @return result list
     */
    default <R> List<R> queryMany(JdbcTemplate jdbcTemplate, String sql, Object[] args, Class<R> rClass) {
        try {
            return jdbcTemplate.queryForList(sql, args, rClass);
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LOGGER.error("[db-error] {}", e.toString());
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", sql, args,
                    ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * 使用{@link JdbcTemplate}执行SQL，获取多条记录并转换为Map类型
     *
     * @param jdbcTemplate {@link JdbcTemplate}
     * @param sql          sql
     * @param args         args
     * @return List&lt;Map&lt;String, Object&gt;&gt;
     */
    default List<Map<String, Object>> queryMany(JdbcTemplate jdbcTemplate, String sql, Object[] args) {
        try {
            return jdbcTemplate.queryForList(sql, args);
        } catch (CannotGetJdbcConnectionException e) {
            LOGGER.error("[db-error] {}", e.toString());
            throw e;
        } catch (DataAccessException e) {
            LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", sql, args,
                    ExceptionUtil.getAllExceptionMsg(e));
            throw e;
        }
    }
    
    /**
     * 将所有的SQL到放到同一个事务中去操作
     *
     * @param transactionTemplate {@link TransactionTemplate}
     * @param jdbcTemplate        {@link JdbcTemplate}
     * @param contexts            {@link List} ModifyRequest list
     * @return {@link Boolean}
     */
    default Boolean update(TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate,
            List<ModifyRequest> contexts) {
        return update(transactionTemplate, jdbcTemplate, contexts, null);
    }
    
    /**
     * execute update operation, to fix #3617.
     *
     * 在一个事务内，将{@link EmbeddedStorageContextHolder#SQL_CONTEXT}中的所有SQL全部执行。
     *
     * @param transactionTemplate {@link TransactionTemplate}
     * @param jdbcTemplate        {@link JdbcTemplate}
     * @param contexts            {@link List} ModifyRequest list
     * @return {@link Boolean}
     */
    default Boolean update(TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate,
            List<ModifyRequest> contexts, BiConsumer<Boolean, Throwable> consumer) {
        boolean updateResult = Boolean.FALSE;
        try {
            /**
             * 将所有的SQL执行放入到一个事务中执行
             */
            updateResult = transactionTemplate.execute(status -> {
                String[] errSql = new String[] {null};
                Object[][] args = new Object[][] {null};
                try {
                    // 将所有的SQL取出并依次执行
                    contexts.forEach(pair -> {
                        errSql[0] = pair.getSql();
                        args[0] = pair.getArgs();
                        boolean rollBackOnUpdateFail = pair.isRollBackOnUpdateFail();
                        LoggerUtils.printIfDebugEnabled(LOGGER, "current sql : {}", errSql[0]);
                        LoggerUtils.printIfDebugEnabled(LOGGER, "current args : {}", args[0]);
                        /**
                         * 执行SQL
                         */
                        int row = jdbcTemplate.update(pair.getSql(), pair.getArgs());
                        /**
                         * 执行失败（有可能数据未发生变更）且设置了回滚，便抛出异常
                         */
                        if (rollBackOnUpdateFail && row < 1) {
                            LoggerUtils.printIfDebugEnabled(LOGGER, "SQL update affected {} rows ", row);
                            throw new IllegalTransactionStateException("Illegal transaction");
                        }
                    });
                    /**
                     * 将结果回写
                     */
                    if (consumer != null) {
                        consumer.accept(Boolean.TRUE, null);
                    }
                    return Boolean.TRUE;
                } catch (BadSqlGrammarException | DataIntegrityViolationException e) {
                    LOGGER.error("[db-error] sql : {}, args : {}, error : {}", errSql[0], args[0], e.toString());
                    /**
                     * 将异常回写
                     */
                    if (consumer != null) {
                        consumer.accept(Boolean.FALSE, e);
                    }
                    return Boolean.FALSE;
                } catch (CannotGetJdbcConnectionException e) {
                    LOGGER.error("[db-error] sql : {}, args : {}, error : {}", errSql[0], args[0], e.toString());
                    throw e;
                } catch (DataAccessException e) {
                    LOGGER.error("[db-error] DataAccessException sql : {}, args : {}, error : {}", errSql[0], args[0],
                            ExceptionUtil.getAllExceptionMsg(e));
                    throw e;
                }
            });
        } catch (IllegalTransactionStateException e) {
            LoggerUtils.printIfDebugEnabled(LOGGER, "Roll back transaction for {} ", e.getMessage());
            if (consumer != null) {
                consumer.accept(Boolean.FALSE, e);
            }
        }
        return updateResult;
    }
    
    /**
     * Perform data import.
     *
     * @param template {@link JdbcTemplate}
     * @param requests {@link List} ModifyRequest list
     * @return {@link Boolean}
     */
    default Boolean doDataImport(JdbcTemplate template, List<ModifyRequest> requests) {
        final String[] sql = requests.stream().map(ModifyRequest::getSql).map(DerbyUtils::insertStatementCorrection)
                .toArray(String[]::new);
        int[] affect = template.batchUpdate(sql);
        return IntStream.of(affect).count() == requests.size();
    }
    
}
