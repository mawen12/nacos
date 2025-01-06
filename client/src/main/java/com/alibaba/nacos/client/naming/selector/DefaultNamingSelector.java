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

package com.alibaba.nacos.client.naming.selector;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.selector.NamingContext;
import com.alibaba.nacos.api.naming.selector.NamingResult;
import com.alibaba.nacos.api.naming.selector.NamingSelector;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 默认的注册中心过滤器
 *
 * @author lideyou
 */
public class DefaultNamingSelector implements NamingSelector {
    /**
     * 过滤实例的条件
     */
    private final Predicate<Instance> filter;
    
    public DefaultNamingSelector(Predicate<Instance> filter) {
        this.filter = filter;
    }
    
    @Override
    public NamingResult select(NamingContext context) {
        /**
         * 获取过滤后的实例列表
         */
        List<Instance> instances = doFilter(context.getInstances());
        /**
         * 返回
         */
        return () -> instances;
    }
    
    private List<Instance> doFilter(List<Instance> instances) {
        /**
         * 使用Stream执行过滤
         */
        return instances == null ? Collections.emptyList() : instances.stream().filter(filter).collect(Collectors.toList());
    }
}
