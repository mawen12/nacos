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

package com.alibaba.nacos.client.config;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.filter.IConfigFilter;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigRequest;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.http.ServerHttpAgent;
import com.alibaba.nacos.client.config.impl.ClientWorker;
import com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor;
import com.alibaba.nacos.client.config.impl.LocalEncryptedDataKeyProcessor;
import com.alibaba.nacos.client.config.impl.ServerListManager;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.ParamUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.PreInitUtils;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Properties;

/**
 * Config Impl.
 *
 * 基于Nacos的配置中心实现
 *
 * @author Nacos
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosConfigService implements ConfigService {
    
    private static final Logger LOGGER = LogUtils.logger(NacosConfigService.class);

    /**
     * 服务状态，代表在线
     */
    private static final String UP = "UP";

    /**
     * 服务状态，代表离线
     */
    private static final String DOWN = "DOWN";
    
    /**
     * will be deleted in 2.0 later versions
     */
    @Deprecated
    ServerHttpAgent agent = null;
    
    /**
     * long polling.
     *
     * 长轮询
     */
    private final ClientWorker worker;

    /**
     * 命名空间
     */
    private String namespace;

    /**
     * 配置过滤器责任链管理器
     */
    private final ConfigFilterChainManager configFilterChainManager;
    
    public NacosConfigService(Properties properties) throws NacosException {
        /**
         * 异步线程初始化 {@link com.fasterxml.jackson.databind.ObjectMapper} 和 {@link com.alibaba.nacos.client.auth.ram.identify.CredentialService}
         */
        PreInitUtils.asyncPreLoadCostComponent();
        /**
         * 构造属性值
         */
        final NacosClientProperties clientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
        /**
         * 日志打印参数
         */
        LOGGER.info(ParamUtil.getInputParameters(clientProperties.asProperties()));
        /**
         * 校验contextPath
         */
        ValidatorUtils.checkInitParam(clientProperties);
        /**
         * 初始化namespace，{@link #namespace}
         */
        initNamespace(clientProperties);
        /**
         * 初始化配置过滤器管理器 {@link #configFilterChainManager}
         */
        this.configFilterChainManager = new ConfigFilterChainManager(clientProperties.asProperties());
        /**
         * 初始化Nacos Server多节点管理器
         */
        ServerListManager serverListManager = new ServerListManager(clientProperties);
        /**
         * 启动多节点管理器
         */
        serverListManager.start();

        /**
         * 启动客户端工作者
         */
        this.worker = new ClientWorker(this.configFilterChainManager, serverListManager, clientProperties);
        // will be deleted in 2.0 later versions
        agent = new ServerHttpAgent(serverListManager);
        
    }

    /**
     * 解析命名空间
     * 解析到 {@link #namespace}
     *       {@link PROPERTIES}
     *
     * @param properties
     */
    private void initNamespace(NacosClientProperties properties) {
        namespace = ParamUtil.parseNamespace(properties);
        properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
    }
    
    @Override
    public String getConfig(String dataId, String group, long timeoutMs) throws NacosException {
        /**
         * 获取默认命名空间中，特定的dataId, group的配置
         */
        return getConfigInner(namespace, dataId, group, timeoutMs);
    }
    
    @Override
    public String getConfigAndSignListener(String dataId, String group, long timeoutMs, Listener listener)
            throws NacosException {
        /**
         * 如果传递的分组为空，则使用 DEFAULT(DEFAULT_GROUP)
         */
        group = StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
        /**
         * 查询特定dataId, group, tenant的配置
         */
        ConfigResponse configResponse = worker.getAgent().queryConfig(dataId, group, worker.getAgent().getTenant(), timeoutMs, false);
        /**
         * 读取配置内容
         */
        String content = configResponse.getContent();
        /**
         * 读取加密密钥，用于解密配置内容
         */
        String encryptedDataKey = configResponse.getEncryptedDataKey();
        /**
         * 将特定dataId, group, tenant的Listener注册到worker上
         */
        worker.addTenantListenersWithContent(dataId, group, content, encryptedDataKey, Collections.singletonList(listener));

        /**
         * 将内容转换为ConfigResponse，用于后续的配置内容过滤
         */
        // get a decryptContent, fix https://github.com/alibaba/nacos/issues/7039
        ConfigResponse cr = new ConfigResponse();
        cr.setDataId(dataId);
        cr.setGroup(group);
        cr.setContent(content);
        cr.setEncryptedDataKey(encryptedDataKey);
        /**
         * 对配置内容进行链式处理，其中默认的是加密解密处理器
         */
        configFilterChainManager.doFilter(null, cr);
        /**
         * 返回经过链式处理的配置内容
         */
        return cr.getContent();
    }
    
    @Override
    public void addListener(String dataId, String group, Listener listener) throws NacosException {
        worker.addTenantListeners(dataId, group, Collections.singletonList(listener));
    }
    
    @Override
    public boolean publishConfig(String dataId, String group, String content) throws NacosException {
        /**
         * 发布配置到服务器端，使用默认类型：TEXT
         */
        return publishConfig(dataId, group, content, ConfigType.getDefaultType().getType());
    }
    
    @Override
    public boolean publishConfig(String dataId, String group, String content, String type) throws NacosException {
        /**
         * 发布配置到服务端的指定命名空间上，不携带标签
         */
        return publishConfigInner(namespace, dataId, group, null, null, null, content, type, null);
    }
    
    @Override
    public boolean publishConfigCas(String dataId, String group, String content, String casMd5) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content,
                ConfigType.getDefaultType().getType(), casMd5);
    }
    
    @Override
    public boolean publishConfigCas(String dataId, String group, String content, String casMd5, String type)
            throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content, type, casMd5);
    }

    @Override
    public boolean removeConfig(String dataId, String group) throws NacosException {
        /**
         * 移除当前租户下特定dataId, group的配置和标签
         */
        return removeConfigInner(namespace, dataId, group, null);
    }
    
    @Override
    public void removeListener(String dataId, String group, Listener listener) {
        worker.removeTenantListener(dataId, group, listener);
    }

    /**
     * 配置读取步骤：
     * 1.从本地的failover读取配置，如果内容存在就直接返回；反之跳到第二步
     *  1.1. 本地的failover是由用户自己配置的
     * 2.从服务器上读取配置，如果请求成功就执行以下子步骤；反之跳到第三步
     *  2.1. 解析响应，获取内容
     *  2.2. 将响应写入本地snapshot
     * 3.从本地的snapshot读取配置，无论是否存在均返回
     *
     * @param tenant
     * @param dataId
     * @param group
     * @param timeoutMs
     * @return
     * @throws NacosException
     */
    private String getConfigInner(String tenant, String dataId, String group, long timeoutMs) throws NacosException {
        /**
         * 如果传递的分组为空，则使用 DEFAULT(DEFAULT_GROUP)
         */
        group = blank2defaultGroup(group);
        /**
         * 校验dataId和group不为空
         */
        ParamUtils.checkKeyParam(dataId, group);
        /**
         * 构造响应，设置dataId, group, tenant
         */
        ConfigResponse cr = new ConfigResponse();
        
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);
        
        // We first try to use local failover content if exists.
        // A config content for failover is not created by client program automatically,
        // but is maintained by user.
        // This is designed for certain scenario like client emergency reboot,
        // changing config needed in the same time, while nacos server is down.
        /**
         * 第一步：尝试从本地读取故障转移文件，如果文件存在，执行第三步，否则直接第二步
         *          故障转移文件的内容不是由程序创建的，而是由用户维护的
         *          这是为某些场景设计的，当Nacos服务器关闭时，客户端紧急重启，
         *          同时需要更改配置。
         */
        String content = LocalConfigInfoProcessor.getFailover(worker.getAgentName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get failover ok, dataId={}, group={}, tenant={}, config={}",
                    worker.getAgentName(), dataId, group, tenant, ContentUtils.truncateContent(content));
            cr.setContent(content);
            String encryptedDataKey = LocalEncryptedDataKeyProcessor.getEncryptDataKeyFailover(agent.getName(), dataId, group, tenant);
            cr.setEncryptedDataKey(encryptedDataKey);
            /**
             * 第四步：读取到配置后，在客户端执行配置过滤器，完成后执行第四步
             */
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();
            return content;
        }

        /**
         * 第二步：发起请求，从服务器端读取特定配置，完成后执行第三步
         */
        try {
            ConfigResponse response = worker.getServerConfig(dataId, group, tenant, timeoutMs, false);
            cr.setContent(response.getContent());
            cr.setEncryptedDataKey(response.getEncryptedDataKey());
            /**
             * 第四步：读取到配置后，在客户端执行配置过滤器，完成后执行第四步
             */
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();
            
            return content;
        } catch (NacosException ioe) {
            if (NacosException.NO_RIGHT == ioe.getErrCode()) {
                throw ioe;
            }
            LOGGER.warn("[{}] [get-config] get from server error, dataId={}, group={}, tenant={}, msg={}",
                    worker.getAgentName(), dataId, group, tenant, ioe.toString());
        }

        /**
         * 第三步：如果服务端出问题，请求出现异常，则从本地快照读取配置
         */
        content = LocalConfigInfoProcessor.getSnapshot(worker.getAgentName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get snapshot ok, dataId={}, group={}, tenant={}, config={}",
                    worker.getAgentName(), dataId, group, tenant, ContentUtils.truncateContent(content));
        }
        cr.setContent(content);
        /**
         * 第四步：读取到配置后，在客户端执行配置过滤器，完成后执行第四步
         */
        String encryptedDataKey = LocalEncryptedDataKeyProcessor.getEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant);
        cr.setEncryptedDataKey(encryptedDataKey);
        configFilterChainManager.doFilter(null, cr);
        content = cr.getContent();
        return content;
    }
    
    private String blank2defaultGroup(String group) {
        return (StringUtils.isBlank(group)) ? Constants.DEFAULT_GROUP : group.trim();
    }
    
    private boolean removeConfigInner(String tenant, String dataId, String group, String tag) throws NacosException {
        /**
         * 处理分组，未指定时取DEFAULT(DEFAULT_GROUP)
         */
        group = blank2defaultGroup(group);
        /**
         * 校验dataId和group合法，
         * - dataId 不为空，合法字符（[a-z][A-Z][0-9][-_.:]）
         * - group 不为空，合法字符（[a-z][A-Z][0-9][-_.:]）
         */
        ParamUtils.checkKeyParam(dataId, group);
        /**
         * 执行移除配置请求
         */
        return worker.removeConfig(dataId, group, tenant, tag);
    }
    
    private boolean publishConfigInner(String tenant, String dataId, String group, String tag, String appName,
            String betaIps, String content, String type, String casMd5) throws NacosException {
        /**
         * 处理分组，未指定时取DEFAULT(DEFAULT_GROUP)
         */
        group = blank2defaultGroup(group);
        /**
         * 校验dataId和group合法，
         * - dataId 不为空，合法字符（[a-z][A-Z][0-9][-_.:]）
         * - group 不为空，合法字符（[a-z][A-Z][0-9][-_.:]）
         */
        ParamUtils.checkParam(dataId, group, content);

        /**
         * 构造配置请求
         */
        ConfigRequest cr = new ConfigRequest();
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);
        cr.setContent(content);
        cr.setType(type);

        /**
         * 使用配置过滤器过滤配置，主要是对配置内容进行加密，并生成对应的密钥，该密钥可以被用来对加密内容进行解密
         */
        configFilterChainManager.doFilter(cr, null);
        content = cr.getContent();
        String encryptedDataKey = cr.getEncryptedDataKey();

        /**
         * 使用底层发布接口，将内容、密钥，md5值发布到服务器端
         */
        return worker.publishConfig(dataId, group, tenant, appName, tag, betaIps, content, encryptedDataKey, casMd5, type);
    }
    
    @Override
    public String getServerStatus() {
        if (worker.isHealthServer()) {
            return UP;
        } else {
            return DOWN;
        }
    }

    @Override
    public void addConfigFilter(IConfigFilter configFilter) {
        configFilterChainManager.addFilter(configFilter);
    }

    @Override
    public void shutDown() throws NacosException {
        worker.shutdown();
    }
}
