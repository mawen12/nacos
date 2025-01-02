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

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.utils.ContextPathUtil;
import com.alibaba.nacos.client.utils.EnvUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import com.alibaba.nacos.common.http.param.Header;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.common.constant.RequestUrlConstants.HTTPS_PREFIX;
import static com.alibaba.nacos.common.constant.RequestUrlConstants.HTTP_PREFIX;

/**
 * Serverlist Manager.
 *
 * 服务列表管理器
 *
 * @author Nacos
 */
public class ServerListManager implements Closeable {
    
    private static final Logger LOGGER = LogUtils.logger(ServerListManager.class);
    
    private final NacosRestTemplate nacosRestTemplate = ConfigHttpClientManager.getInstance().getNacosRestTemplate();
    
    private final ScheduledExecutorService executorService =
            new ScheduledThreadPoolExecutor(1, new NameThreadFactory("com.alibaba.nacos.client.ServerListManager"));
    
    /**
     * The name of the different environment.
     */
    private final String name;
    
    private String namespace = "";
    
    private String tenant = "";
    
    public static final String DEFAULT_NAME = "default";
    
    public static final String CUSTOM_NAME = "custom";
    
    public static final String FIXED_NAME = "fixed";
    
    private final int initServerListRetryTimes = 5;
    
    final boolean isFixed;

    /**
     * 多节点管理器启动标识，true标识已启动；反之表示未启动
     */
    boolean isStarted;
    
    private String endpoint;
    
    private int endpointPort = 8080;
    
    private String endpointContextPath;
    
    private String contentPath = ParamUtil.getDefaultContextPath();
    
    private String serverListName = ParamUtil.getDefaultNodesPath();
    
    volatile List<String> serverUrls = new ArrayList<>();
    
    private volatile String currentServerAddr;
    
    private Iterator<String> iterator;
    
    public String addressServerUrl;
    
    private String serverAddrsStr;
    
    public ServerListManager() {
        this.isFixed = false;
        this.isStarted = false;
        this.name = DEFAULT_NAME;
    }
    
    public ServerListManager(List<String> fixed) {
        this(fixed, null);
    }
    
    public ServerListManager(List<String> fixed, String namespace) {
        this.isFixed = true;
        this.isStarted = true;
        List<String> serverAddrs = new ArrayList<>();
        for (String serverAddr : fixed) {
            String[] serverAddrArr = InternetAddressUtil.splitIPPortStr(serverAddr);
            if (serverAddrArr.length == 1) {
                serverAddrs
                        .add(serverAddrArr[0] + InternetAddressUtil.IP_PORT_SPLITER + ParamUtil.getDefaultServerPort());
            } else {
                serverAddrs.add(serverAddr);
            }
        }
        this.serverUrls = new ArrayList<>(serverAddrs);
        if (StringUtils.isNotBlank(namespace)) {
            this.namespace = namespace;
            this.tenant = namespace;
        }
        this.name = initServerName(null);
    }
    
    public ServerListManager(String host, int port) {
        this.isFixed = false;
        this.isStarted = false;
        this.endpoint = host;
        this.endpointPort = port;
        
        this.name = initServerName(null);
        initAddressServerUrl(null);
    }
    
    public ServerListManager(String endpoint) throws NacosException {
        this(endpoint, null);
    }
    
    public ServerListManager(String endpoint, String namespace) throws NacosException {
        this.isFixed = false;
        this.isStarted = false;
        if (StringUtils.isBlank(endpoint)) {
            throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "endpoint is blank");
        }
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.ENDPOINT, endpoint);
        final NacosClientProperties clientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
        initParam(clientProperties);
        if (StringUtils.isNotBlank(namespace)) {
            this.namespace = namespace;
            this.tenant = namespace;
        }
        this.name = initServerName(null);
        initAddressServerUrl(clientProperties);
    }
    
    public ServerListManager(NacosClientProperties properties) throws NacosException {
        /**
         * 启动状态标志位
         */
        this.isStarted = false;
        /**
         * 初始化参数信息
         */
        initParam(properties);

        if (StringUtils.isNotEmpty(serverAddrsStr)) {
            /**
             * 如果提供了serverAddr，则从该值中解析
             */
            this.isFixed = true;
            List<String> serverAddrs = new ArrayList<>();
            StringTokenizer serverAddrsTokens = new StringTokenizer(this.serverAddrsStr, ",;");
            while (serverAddrsTokens.hasMoreTokens()) {
                String serverAddr = serverAddrsTokens.nextToken().trim();
                if (serverAddr.startsWith(HTTP_PREFIX) || serverAddr.startsWith(HTTPS_PREFIX)) {
                    /**
                     * 传递的服务地址中包含了http或https协议，则直接使用
                     */
                    serverAddrs.add(serverAddr);
                } else {
                    /**
                     * 传递的服务地址中只有ip:port，则在前面追加https协议，支持ipv4和ipv6
                     */
                    String[] serverAddrArr = InternetAddressUtil.splitIPPortStr(serverAddr);
                    /**
                     * 未设置端口时，采用默认的8848端口
                     */
                    if (serverAddrArr.length == 1) {
                        serverAddrs.add(HTTP_PREFIX + serverAddrArr[0] + InternetAddressUtil.IP_PORT_SPLITER + ParamUtil
                                .getDefaultServerPort());
                    } else {
                        /**
                         * 已设置端口，直接在前面设置http
                         */
                        serverAddrs.add(HTTP_PREFIX + serverAddr);
                    }
                }
            }
            this.serverUrls = serverAddrs;
            /**
             * 初始化服务名
             */
            this.name = initServerName(properties);
        } else {
            /**
             * 未指定serverAddr时，就从endpoint中读取Nacos Server信息，如果endpoint也未设置，则报错
             */
            if (StringUtils.isBlank(endpoint)) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "endpoint is blank");
            }
            /**
             * 解析标志位设置为失败
             */
            this.isFixed = false;
            /**
             * 解析服务名称
             * 解析操作为 从配置文件中读取serverName -> fixed+namespace+serverUrls | custom-endpoint-contextPath-serverListName-namespace
             * 解析到{@link name}
             */
            this.name = initServerName(properties);
            /**
             * 解析Nacos Server地址路径
             * 解析到{@link addressServerUrl}
             */
            initAddressServerUrl(properties);
        }
    }

    /**
     * 初始化命名空间和租户
     *
     * @param properties
     */
    private void initNameSpace(NacosClientProperties properties) {
        String namespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        if (StringUtils.isNotBlank(namespace)) {
            this.namespace = namespace;
            this.tenant = namespace;
        }
    }

    /**
     * 初始化服务地址
     *
     * @param properties
     */
    private void initServerAddr(NacosClientProperties properties) {
        this.serverAddrsStr = properties.getProperty(PropertyKeyConst.SERVER_ADDR);
    }

    /**
     * 初始化服务名称
     *
     * @param properties
     * @return
     */
    private String initServerName(NacosClientProperties properties) {
        String serverName;
        //1.user define server name.
        if (properties != null && properties.containsKey(PropertyKeyConst.SERVER_NAME)) {
            serverName = properties.getProperty(PropertyKeyConst.SERVER_NAME);
        } else {
            // if fix url, use fix url join string.
            if (isFixed) {
                serverName = FIXED_NAME + "-" + (StringUtils.isNotBlank(namespace) ? (StringUtils.trim(namespace) + "-")
                        : "") + getFixedNameSuffix(serverUrls.toArray(new String[0]));
            } else {
                //if use endpoint, use endpoint, content path, serverList name
                String contextPathTmp =
                        StringUtils.isNotBlank(this.endpointContextPath) ? this.endpointContextPath : this.contentPath;
                serverName =
                        CUSTOM_NAME + "-" + String.join("_", endpoint, String.valueOf(endpointPort), contextPathTmp,
                                serverListName) + (StringUtils.isNotBlank(namespace) ? ("_" + StringUtils.trim(
                                namespace)) : "");
            }
        }
        serverName = serverName.replaceAll("\\/", "_");
        serverName = serverName.replaceAll("\\:", "_");
        return serverName;
    }

    /**
     * 初始化服务器的http地址
     *
     * @param properties
     */
    private void initAddressServerUrl(NacosClientProperties properties) {
        if (isFixed) {
            return;
        }
        String contextPathTem = StringUtils.isNotBlank(this.endpointContextPath) ? ContextPathUtil.normalizeContextPath(
                this.endpointContextPath) : ContextPathUtil.normalizeContextPath(this.contentPath);
        StringBuilder addressServerUrlTem = new StringBuilder(
                String.format("http://%s:%d%s/%s", this.endpoint, this.endpointPort, contextPathTem,
                        this.serverListName));
        boolean hasQueryString = false;
        if (StringUtils.isNotBlank(namespace)) {
            addressServerUrlTem.append("?namespace=").append(namespace);
            hasQueryString = true;
        }
        if (properties != null && properties.containsKey(PropertyKeyConst.ENDPOINT_QUERY_PARAMS)) {
            addressServerUrlTem.append(
                    hasQueryString ? "&" : "?" + properties.getProperty(PropertyKeyConst.ENDPOINT_QUERY_PARAMS));
            
        }
        this.addressServerUrl = addressServerUrlTem.toString();
        LOGGER.info("serverName = {},  address server url = {}", this.name, this.addressServerUrl);
    }
    
    private void initParam(NacosClientProperties properties) {
        /**
         * 解析Nacos Server地址，解析到 {@link serverAddrsStr}
         */
        initServerAddr(properties);
        /**
         * 解析Nacos Server命名空间，解析到 {@link namespace}和{@link tenant}
         */
        initNameSpace(properties);
        /**
         * 解析Nacos Server端点，解析到{@link endpoint}
         */
        initEndpoint(properties);
        /**
         * 解析Nacos Server端点端口，解析到{@link endpointPort}
         */
        initEndpointPort(properties);
        /**
         * 解析Nacos Server端点端口上下文路径，解析到{@link endpointContextPath}
         */
        initEndpointContextPath(properties);
        /**
         * 解析Nacos Server上下文路径，解析到{@link contentPath}
         */
        initContextPath(properties);
        /**
         * 解析Nacos Server服务列表名称，解析到{@link serverListName}
         */
        initServerListName(properties);
    }

    /**
     * 初始化端点上下文路径
     *
     * @param properties
     */
    private void initEndpointContextPath(NacosClientProperties properties) {
        String endpointContextPathTmp = TemplateUtils.stringEmptyAndThenExecute(
                properties.getProperty(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_CONTEXT_PATH),
                () -> properties.getProperty(PropertyKeyConst.ENDPOINT_CONTEXT_PATH));
        if (StringUtils.isNotBlank(endpointContextPathTmp)) {
            this.endpointContextPath = endpointContextPathTmp;
        }
    }

    /**
     * 初始化端点接口
     *
     * @param properties
     */
    private void initEndpointPort(NacosClientProperties properties) {
        String endpointPortTmp = TemplateUtils.stringEmptyAndThenExecute(
                properties.getProperty(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_PORT),
                () -> properties.getProperty(PropertyKeyConst.ENDPOINT_PORT));
        if (StringUtils.isNotBlank(endpointPortTmp)) {
            this.endpointPort = Integer.parseInt(endpointPortTmp);
        }
    }

    /**
     * 初始化服务器列表名称
     *
     * @param properties
     */
    private void initServerListName(NacosClientProperties properties) {
        String serverListNameTmp = properties.getProperty(PropertyKeyConst.ENDPOINT_CLUSTER_NAME,
                properties.getProperty(PropertyKeyConst.CLUSTER_NAME));
        if (!StringUtils.isBlank(serverListNameTmp)) {
            this.serverListName = serverListNameTmp;
        }
    }

    /**
     * 初始化下文件路径
     *
     * @param properties
     */
    private void initContextPath(NacosClientProperties properties) {
        String contentPathTmp = properties.getProperty(PropertyKeyConst.CONTEXT_PATH);
        if (!StringUtils.isBlank(contentPathTmp)) {
            this.contentPath = contentPathTmp;
        }
    }

    /**
     * 初始化端点
     *
     * @param properties
     */
    private void initEndpoint(final NacosClientProperties properties) {
        String endpointTmp = properties.getProperty(PropertyKeyConst.ENDPOINT);
        // Whether to enable domain name resolution rules
        String isUseEndpointRuleParsing = properties.getProperty(PropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                properties.getProperty(SystemPropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                        String.valueOf(ParamUtil.USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE)));
        if (Boolean.parseBoolean(isUseEndpointRuleParsing)) {
            String endpointUrl = ParamUtil.parsingEndpointRule(endpointTmp);
            if (StringUtils.isNotBlank(endpointUrl)) {
                this.serverAddrsStr = "";
            }
            endpointTmp = endpointUrl;
        }
        this.endpoint = StringUtils.isNotBlank(endpointTmp) ? endpointTmp : "";
    }
    
    /**
     * Start.
     *
     * @throws NacosException nacos exception
     */
    public synchronized void start() throws NacosException {
        /**
         * 已经启动，或者直接设置了serverAddrStr，表示无需启动
         */
        if (isStarted || isFixed) {
            return;
        }
        /**
         * 构建基于地址路径，从目标服务端获取信息的任务
         */
        GetServerListTask getServersTask = new GetServerListTask(addressServerUrl);
        /**
         * 最大重试5次，且未查询到结果
         */
        for (int i = 0; i < initServerListRetryTimes && serverUrls.isEmpty(); ++i) {
            getServersTask.run();
            if (!serverUrls.isEmpty()) {
                break;
            }
            try {
                // 失败后等待 100 -> 200 -> 300 -> 400 -> 500 毫秒
                this.wait((i + 1) * 100L);
            } catch (Exception e) {
                LOGGER.warn("get serverlist fail,url: {}", addressServerUrl);
            }
        }

        /**
         * 在重试5次后，仍未查询到结果，则抛出异常
         */
        if (serverUrls.isEmpty()) {
            LOGGER.error("[init-serverlist] fail to get NACOS-server serverlist! env: {}, url: {}", name,
                    addressServerUrl);
            throw new NacosException(NacosException.SERVER_ERROR,
                    "fail to get NACOS-server serverlist! env:" + name + ", not connnect url:" + addressServerUrl);
        }

        // executor schedules the timer task
        /**
         * 使用定时任务，在后台每隔30秒更新{@link serverUrls}
         */
        this.executorService.scheduleWithFixedDelay(getServersTask, 0L, 30L, TimeUnit.SECONDS);
        isStarted = true;
    }
    
    public List<String> getServerUrls() {
        return serverUrls;
    }
    
    Iterator<String> iterator() {
        if (serverUrls.isEmpty()) {
            LOGGER.error("[{}] [iterator-serverlist] No server address defined!", name);
        }
        return new ServerAddressIterator(serverUrls);
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, LOGGER);
        LOGGER.info("{} do shutdown stop", className);
    }
    
    class GetServerListTask implements Runnable {
        
        final String url;
        
        GetServerListTask(String url) {
            this.url = url;
        }
        
        @Override
        public void run() {
            /*
             get serverlist from nameserver
             */
            try {
                updateIfChanged(getApacheServerList(url, name));
            } catch (Exception e) {
                LOGGER.error("[" + name + "][update-serverlist] failed to update serverlist from address server!", e);
            }
        }
    }
    
    private void updateIfChanged(List<String> newList) {
        /**
         * 当任务执行完毕后，未获取到Nacos Server的集群节点信息，则不做任何处理
         */
        if (null == newList || newList.isEmpty()) {
            LOGGER.warn("[update-serverlist] current serverlist from address server is empty!!!");
            return;
        }

        /**
         * 对获取到的节点信息进行加工，如果未指定http或https协议，则进行补充，并添加到列表中。
         */
        List<String> newServerAddrList = new ArrayList<>();
        for (String server : newList) {
            if (server.startsWith(HTTP_PREFIX) || server.startsWith(HTTPS_PREFIX)) {
                newServerAddrList.add(server);
            } else {
                newServerAddrList.add(HTTP_PREFIX + server);
            }
        }
        
        /*
         no change
         */
        /**
         * 如果列表内容相等，则不做任何处理
         */
        if (newServerAddrList.equals(serverUrls)) {
            return;
        }
        /**
         * 更新 {@link serverUrls}
         */
        serverUrls = new ArrayList<>(newServerAddrList);
        /**
         * 更新迭代器
         */
        iterator = iterator();
        /**
         * 获取迭代器中的下一个节点地址
         */
        currentServerAddr = iterator.next();
        
        // Using unified event processor, NotifyCenter
        /**
         * 发布服务列表变更事件
         */
        NotifyCenter.publishEvent(new ServerListChangeEvent());
        LOGGER.info("[{}] [update-serverList] serverList updated to {}", name, serverUrls);
    }
    
    private List<String> getApacheServerList(String url, String name) {
        try {
            HttpRestResult<String> httpResult = nacosRestTemplate.get(url, Header.EMPTY, Query.EMPTY, String.class);
            
            if (httpResult.ok()) {
                if (DEFAULT_NAME.equals(name)) {
                    EnvUtil.setSelfEnv(httpResult.getHeader().getOriginalResponseHeader());
                }
                List<String> lines = IoUtils.readLines(new StringReader(httpResult.getData()));
                List<String> result = new ArrayList<>(lines.size());
                for (String serverAddr : lines) {
                    if (StringUtils.isNotBlank(serverAddr)) {
                        String[] ipPort = InternetAddressUtil.splitIPPortStr(serverAddr.trim());
                        String ip = ipPort[0].trim();
                        if (ipPort.length == 1) {
                            result.add(ip + InternetAddressUtil.IP_PORT_SPLITER + ParamUtil.getDefaultServerPort());
                        } else {
                            result.add(serverAddr);
                        }
                    }
                }
                return result;
            } else {
                LOGGER.error("[check-serverlist] error. addressServerUrl: {}, code: {}", addressServerUrl,
                        httpResult.getCode());
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("[check-serverlist] exception. url: " + url, e);
            return null;
        }
    }
    
    String getUrlString() {
        return serverUrls.toString();
    }
    
    String getFixedNameSuffix(String... serverIps) {
        StringBuilder sb = new StringBuilder();
        String split = "";
        for (String serverIp : serverIps) {
            sb.append(split);
            serverIp = serverIp.replaceAll("http(s)?://", "");
            sb.append(serverIp.replaceAll(":", "_"));
            split = "-";
        }
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return "ServerManager-" + name + "-" + getUrlString();
    }
    
    public boolean contain(String ip) {
        
        return serverUrls.contains(ip);
    }
    
    public void refreshCurrentServerAddr() {
        iterator = iterator();
        currentServerAddr = iterator.next();
    }
    
    public String getNextServerAddr() {
        if (iterator == null || !iterator.hasNext()) {
            refreshCurrentServerAddr();
            return currentServerAddr;
        }
        try {
            return iterator.next();
        } catch (Exception e) {
            //No nothing.
        }
        refreshCurrentServerAddr();
        return currentServerAddr;
        
    }
    
    public String getCurrentServerAddr() {
        if (StringUtils.isBlank(currentServerAddr)) {
            iterator = iterator();
            currentServerAddr = iterator.next();
        }
        return currentServerAddr;
    }
    
    public void updateCurrentServerAddr(String currentServerAddr) {
        this.currentServerAddr = currentServerAddr;
    }
    
    public Iterator<String> getIterator() {
        return iterator;
    }
    
    public String getContentPath() {
        return contentPath;
    }
    
    public String getName() {
        return name;
    }
    
    public String getNamespace() {
        return namespace;
    }
    
    public String getTenant() {
        return tenant;
    }
    
    /**
     * Sort the address list, with the same room priority.
     */
    private static class ServerAddressIterator implements Iterator<String> {
        
        static class RandomizedServerAddress implements Comparable<RandomizedServerAddress> {
            
            static Random random = new Random();
            
            String serverIp;
            
            int priority = 0;
            
            int seed;
            
            public RandomizedServerAddress(String ip) {
                try {
                    this.serverIp = ip;
                    /*
                     change random scope from 32 to Integer.MAX_VALUE to fix load balance issue
                     */
                    this.seed = random.nextInt(Integer.MAX_VALUE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            
            @Override
            public int compareTo(RandomizedServerAddress other) {
                if (this.priority != other.priority) {
                    return other.priority - this.priority;
                } else {
                    return other.seed - this.seed;
                }
            }
        }
        
        public ServerAddressIterator(List<String> source) {
            sorted = new ArrayList<>();
            for (String address : source) {
                sorted.add(new RandomizedServerAddress(address));
            }
            Collections.sort(sorted);
            iter = sorted.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }
        
        @Override
        public String next() {
            return iter.next().serverIp;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        final List<RandomizedServerAddress> sorted;
        
        final Iterator<RandomizedServerAddress> iter;
    }
}
