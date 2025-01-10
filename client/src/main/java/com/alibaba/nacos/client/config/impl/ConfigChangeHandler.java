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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.config.ConfigChangeItem;
import com.alibaba.nacos.api.config.listener.ConfigChangeParser;
import com.alibaba.nacos.common.spi.NacosServiceLoader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 单例设计模式。
 * 配置变更处理程序，
 *
 * @author rushsky518
 */
public class ConfigChangeHandler {

    private final List<ConfigChangeParser> parserList;
    
    private ConfigChangeHandler() {
        this.parserList = new LinkedList<>();
        /**
         * 通过SPI来查找类，默认情况下不会加载任何类，因此若要监听配置变更，只能使用Properties或Yaml。
         * 也可以通过自定义的方式来实现特殊类型的比较。
         */
        Collection<ConfigChangeParser> loader = NacosServiceLoader.load(ConfigChangeParser.class);
        this.parserList.addAll(loader);

        /**
         * 默认支持Properties和Yml配置类型
         */
        this.parserList.add(new PropertiesChangeParser());
        this.parserList.add(new YmlChangeParser());
    }

    /**
     * 返回当前单例
     * @return
     */
    public static ConfigChangeHandler getInstance() {
        return ConfigChangeHandlerHolder.INSTANCE;
    }
    
    /**
     * 对新旧内容进行比较，并返回发生变化（ADDED, MODIFIED, DELETED）的配置项
     *
     * @param oldContent old data
     * @param newContent new data
     * @param type       data type
     * @return Map<ADDED/MODIFIED/DELETED, 配置变更元素>
     * @throws IOException io exception
     */
    public Map<String, ConfigChangeItem> parseChangeData(String oldContent, String newContent, String type) throws IOException {
        /**
         * 使用具体解析器来解析对应的前后差异
         */
        for (ConfigChangeParser changeParser : this.parserList) {
            if (changeParser.isResponsibleFor(type)) {
                return changeParser.doParse(oldContent, newContent, type);
            }
        }
        
        return Collections.emptyMap();
    }

    private static class ConfigChangeHandlerHolder {

        private static final ConfigChangeHandler INSTANCE = new ConfigChangeHandler();
    }
}
