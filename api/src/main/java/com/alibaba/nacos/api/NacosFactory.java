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

package com.alibaba.nacos.api;

import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingMaintainFactory;
import com.alibaba.nacos.api.naming.NamingMaintainService;
import com.alibaba.nacos.api.naming.NamingService;

import java.util.Properties;

/**
 * Nacos Factory.
 *
 * 简单工厂设计模式。
 * Nacos工厂，负责初始化配置中心和注册中心。
 *
 * @author Nacos
 */
public class NacosFactory {

    /**
     * 创建配置服务
     *
     * @param properties init param
     *
     * @return config
     *
     * @throws NacosException Exception
     */
    public static ConfigService createConfigService(Properties properties) throws NacosException {
        return ConfigFactory.createConfigService(properties);
    }

    /**
     * 创建配置服务
     *
     * @param serverAddr server list
     *
     * @return config
     *
     * @throws NacosException Exception
     */
    public static ConfigService createConfigService(String serverAddr) throws NacosException {
        return ConfigFactory.createConfigService(serverAddr);
    }

    /**
     * 创建命名服务
     *
     * @param serverAddr server list
     *
     * @return Naming
     *
     * @throws NacosException Exception
     */
    public static NamingService createNamingService(String serverAddr) throws NacosException {
        return NamingFactory.createNamingService(serverAddr);
    }

    /**
     * 创建命名服务
     *
     * @param properties init param
     *
     * @return Naming
     *
     * @throws NacosException Exception
     */
    public static NamingService createNamingService(Properties properties) throws NacosException {
        return NamingFactory.createNamingService(properties);
    }

    /**
     * Create maintain service.
     *
     * @param serverAddr server address
     *
     * @return NamingMaintainService
     *
     * @throws NacosException Exception
     */
    public static NamingMaintainService createMaintainService(String serverAddr) throws NacosException {
        return NamingMaintainFactory.createMaintainService(serverAddr);
    }

    /**
     * Create maintain service.
     *
     * @param properties server address
     *
     * @return NamingMaintainService
     *
     * @throws NacosException Exception
     */
    public static NamingMaintainService createMaintainService(Properties properties) throws NacosException {
        return NamingMaintainFactory.createMaintainService(properties);
    }
}
