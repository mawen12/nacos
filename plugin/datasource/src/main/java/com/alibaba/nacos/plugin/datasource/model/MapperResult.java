/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.plugin.datasource.model;

import java.util.List;

/**
 * The object returned by the execution of the Mapper method.
 *
 * 保存Mapper方法执行的返回结果，该Mapper仅负责构造SQL，已经将对应的参数放到数组中。
 * 底层的SQL执行由JdbcTemplate执行
 *
 * @author hyx
 **/

public class MapperResult {
    
    public MapperResult() { }
    
    public MapperResult(String sql, List<Object> paramList) {
        this.sql = sql;
        this.paramList = paramList;
    }

    /**
     * 执行的SQL
     */
    private String sql;

    /**
     * 参数对象
     */
    private List<Object> paramList;
    
    public String getSql() {
        return sql;
    }
    
    public void setSql(String sql) {
        this.sql = sql;
    }
    
    public List<Object> getParamList() {
        return paramList;
    }
    
    public void setParamList(List<Object> paramList) {
        this.paramList = paramList;
    }
    
    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public String toString() {
        return "MapperResult{" + "sql='" + sql + '\'' + ", paramList=" + paramList + '}';
    }
}
