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

package com.alibaba.nacos.client.naming.utils;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;

import com.alibaba.nacos.client.env.NacosClientProperties;

/**
 * 本地缓存路径，像是灾难恢复文件和注册中心服务都存放在此
 *
 * @author zongkang.guo
 */
public class CacheDirUtil {

    /**
     * 默认的缓存路径为${user.home}/nacos/naming/public
     */
    private static String cacheDir;
    
    private static final String JM_SNAPSHOT_PATH_PROPERTY = "JM.SNAPSHOT.PATH";
    
    private static final String FILE_PATH_NACOS = "nacos";
    
    private static final String FILE_PATH_NAMING = "naming";
    
    private static final String USER_HOME_PROPERTY = "user.home";
    
    /**
     * Init cache dir.
     *
     * @param namespace  namespace.
     * @param properties nacosClientProperties.
     * @return
     */
    public static String initCacheDir(String namespace, NacosClientProperties properties) {
        /**
         * 从 ENV(JM.SNAPSHOT.PATH) 解析快照文件路径
         */
        String jmSnapshotPath = properties.getProperty(JM_SNAPSHOT_PATH_PROPERTY);
        
        String namingCacheRegistryDir = "";
        /**
         * 从 PROPERTIES(namingCacheRegistryDir) 解析缓存的注册目录
         *
         * TODO by mawen 替换为 containsKey
         */
        if (properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR) != null) {
            namingCacheRegistryDir = File.separator + properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR);
        }

        if (!StringUtils.isBlank(jmSnapshotPath)) {
            /**
             * 从 ENV(JM.SNAPSHOT.PATH) + DEFAULT(nacos) + PROPERTIES(namingCacheRegistryDir) + DEFAULT(naming) + PROPERTIES(namespace) 构造缓存目录
             */
            cacheDir = jmSnapshotPath + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        } else {
            /**
             * 从 ENV(user. home) + DEFAULT(nacos) + PROPERTIES(namingCacheRegistryDir) + DEFAULT(naming) + PROPERTIES(namespace) 构造缓存目录
             */
            cacheDir = properties.getProperty(USER_HOME_PROPERTY) + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        }
        
        return cacheDir;
    }
    
    public static String getCacheDir() {
        return cacheDir;
    }
}
