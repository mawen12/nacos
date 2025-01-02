/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.config.filter.impl;

import com.alibaba.nacos.api.config.filter.IConfigFilter;
import com.alibaba.nacos.api.config.filter.IConfigFilterChain;
import com.alibaba.nacos.api.config.filter.IConfigRequest;
import com.alibaba.nacos.api.config.filter.IConfigResponse;
import com.alibaba.nacos.api.exception.NacosException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Config Filter Chain Management.
 *
 * 责任链设计模式。
 * 配置过滤器链管理器
 *
 * @author Nacos
 */
public class ConfigFilterChainManager implements IConfigFilterChain {

    /**
     * 过滤器链
     */
    private final List<IConfigFilter> filters = new ArrayList<>();

    /**
     * 启动参数
     */
    private final Properties initProperty;
    
    public ConfigFilterChainManager(Properties properties) {
        this.initProperty = properties;
        /**
         * 加载配置过滤器，并将其添加到{@link filters}
         */
        ServiceLoader<IConfigFilter> configFilters = ServiceLoader.load(IConfigFilter.class);
        for (IConfigFilter configFilter : configFilters) {
            addFilter(configFilter);
        }
    }
    
    /**
     * Add filter.
     *
     * @param filter filter
     * @return this
     */
    public synchronized ConfigFilterChainManager addFilter(IConfigFilter filter) {
        /**
         * 配置过滤器初始化
         */
        filter.init(this.initProperty);
        // ordered by order value
        int i = 0;
        while (i < this.filters.size()) {
            IConfigFilter currentValue = this.filters.get(i);
            /**
             * 如果要添加的配置过滤器已经加入了，则跳过
             */
            if (currentValue.getFilterName().equals(filter.getFilterName())) {
                break;
            }
            /**
             * 比较过滤器优先级，如果高于当前优先级，则往后迭代；反之加入到集合中，并结束处理
             */
            if (filter.getOrder() >= currentValue.getOrder() && i < this.filters.size()) {
                i++;
            } else {
                this.filters.add(i, filter);
                break;
            }
        }

        /**
         * 要添加的过滤器比列表中所有的过滤器等级要高，或者当前列表中为空
         */
        if (i == this.filters.size()) {
            this.filters.add(i, filter);
        }
        return this;
    }
    
    @Override
    public void doFilter(IConfigRequest request, IConfigResponse response) throws NacosException {
        new VirtualFilterChain(this.filters).doFilter(request, response);
    }

    private static class VirtualFilterChain implements IConfigFilterChain {
        
        private final List<? extends IConfigFilter> additionalFilters;
        
        private int currentPosition = 0;
        
        public VirtualFilterChain(List<? extends IConfigFilter> additionalFilters) {
            this.additionalFilters = additionalFilters;
        }
        
        @Override
        public void doFilter(final IConfigRequest request, final IConfigResponse response) throws NacosException {
            if (this.currentPosition != this.additionalFilters.size()) {
                this.currentPosition++;
                IConfigFilter nextFilter = this.additionalFilters.get(this.currentPosition - 1);
                nextFilter.doFilter(request, response, this);
            }
        }
    }

}
