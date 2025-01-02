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
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.config.remote.request.ClientConfigMetricRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigChangeNotifyRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigPublishRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigQueryRequest;
import com.alibaba.nacos.api.config.remote.request.ConfigRemoveRequest;
import com.alibaba.nacos.api.config.remote.response.ClientConfigMetricResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeBatchListenResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeNotifyResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigPublishResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigQueryResponse;
import com.alibaba.nacos.api.config.remote.response.ConfigRemoveResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.env.SourceType;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.client.utils.EnvUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.labels.impl.DefaultLabelsCollectorManager;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientFactory;
import com.alibaba.nacos.common.remote.client.RpcClientTlsConfig;
import com.alibaba.nacos.common.remote.client.RpcClientTlsConfigFactory;
import com.alibaba.nacos.common.remote.client.ServerListFactory;
import com.alibaba.nacos.common.utils.ConnLabelsUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.alibaba.nacos.plugin.auth.api.RequestResource;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.nacos.api.common.Constants.APP_CONN_PREFIX;
import static com.alibaba.nacos.api.common.Constants.ENCODE;

/**
 * Long polling.
 * <p>
 * 长轮询
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    private static final String NOTIFY_HEADER = "notify";

    private static final String TAG_PARAM = "tag";

    private static final String APP_NAME_PARAM = "appName";

    private static final String BETAIPS_PARAM = "betaIps";

    private static final String TYPE_PARAM = "type";

    private static final String ENCRYPTED_DATA_KEY_PARAM = "encryptedDataKey";

    /**
     * groupKey -> cacheData.
     */
    private final AtomicReference<Map<String, CacheData>> cacheMap = new AtomicReference<>(new HashMap<>());

    private final DefaultLabelsCollectorManager defaultLabelsCollectorManager = new DefaultLabelsCollectorManager();

    private Map<String, String> appLables = new HashMap<>();

    private final ConfigFilterChainManager configFilterChainManager;

    private final String uuid = UUID.randomUUID().toString();

    private long timeout;

    private final ConfigRpcTransportClient agent;

    private int taskPenaltyTime;

    private boolean enableRemoteSyncConfig = false;

    private static final int MIN_THREAD_NUM = 2;

    private static final int THREAD_MULTIPLE = 1;

    /**
     * index(taskId)-> total cache count for this taskId.
     */
    private final List<AtomicInteger> taskIdCacheCountList = new ArrayList<>();

    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) throws NacosException {
        group = blank2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        synchronized (cache) {
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            if (getCache(dataId, group) != cache) {
                putCache(GroupKey.getKey(dataId, group), cache);
            }
            agent.notifyListenConfig();
        }
    }

    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        synchronized (cache) {
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            cache.setDiscard(false);
            cache.setConsistentWithServer(false);
            // ensure cache present in cacheMap
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            agent.notifyListenConfig();
        }

    }

    /**
     * Add listeners for tenant with content.
     *
     * @param dataId           dataId of data
     * @param group            group of data
     * @param content          content
     * @param encryptedDataKey encryptedDataKey
     * @param listeners        listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content, String encryptedDataKey,
                                              List<? extends Listener> listeners) throws NacosException {
        /**
         * 如果传递的分组为空，则使用 DEFAULT(DEFAULT_GROUP)
         */
        group = blank2defaultGroup(group);
        /**
         * 获取命名空间
         */
        String tenant = agent.getTenant();
        /**
         * 获取特定dataId, group, tenant的缓存，如果缓存不存在，则创建并返回
         */
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);

        synchronized (cache) {
            /**
             * 将刚才请求返回的配置内容和加密密钥写入缓存中
             */
            cache.setEncryptedDataKey(encryptedDataKey);
            cache.setContent(content);
            /**
             * 将监听器添加到缓存中
             */
            for (Listener listener : listeners) {
                cache.addListener(listener);
            }
            /**
             * 当缓存上注册了监听器，此时该缓存是不可丢弃的，因为客户端需要该缓存在配置更新时，通知到对应的监听器
             */
            cache.setDiscard(false);
            /**
             * 默认缓存无需与服务端保持一致
             */
            cache.setConsistentWithServer(false);
            // make sure cache exists in cacheMap
            /**
             * 确保缓存映射中已经存在该缓存
             */
            if (getCache(dataId, group, tenant) != cache) {
                putCache(GroupKey.getKeyTenant(dataId, group, tenant), cache);
            }
            /**
             *
             */
            agent.notifyListenConfig();
        }

    }

    /**
     * 移除缓存上的指定监听器
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            synchronized (cache) {
                /**
                 * 从缓存中移除监听器
                 */
                cache.removeListener(listener);
                /**
                 * 如果监听器为空，就代表该缓存无需同服务器端同步配置，且缓存是可丢弃的。
                 * 并通知服务器端清除对应的信息
                 */
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }

        }
    }

    /**
     * 移除缓存上特定租户的指定监听器，该租户是在服务启动时，指定在启动配置中的。
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = blank2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            synchronized (cache) {
                /**
                 * 从缓存中移除监听器
                 */
                cache.removeListener(listener);
                /**
                 * 如果监听器为空，就代表该缓存无需同服务器端同步配置，且缓存是可丢弃的。
                 * 并通知服务器端清除对应的信息
                 */
                if (cache.getListeners().isEmpty()) {
                    cache.setConsistentWithServer(false);
                    cache.setDiscard(true);
                    agent.removeCache(dataId, group);
                }
            }
        }
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            CacheData remove = copy.remove(groupKey);
            if (remove != null) {
                decreaseTaskIdCount(remove.getTaskId());
            }
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());
    }

    /**
     * remove config.
     *
     * @param dataId dataId.
     * @param group  group.
     * @param tenant tenant.
     * @param tag    tag.
     * @return success or not.
     * @throws NacosException exception to throw.
     */
    public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
        /**
         * 使用RPC客户端发起请求
         */
        return agent.removeConfig(dataId, group, tenant, tag);
    }

    /**
     * publish config.
     *
     * @param dataId  dataId.
     * @param group   group.
     * @param tenant  tenant.
     * @param appName appName.
     * @param tag     tag.
     * @param betaIps betaIps.
     * @param content content.
     * @param casMd5  casMd5.
     * @param type    type.
     * @return success or not.
     * @throws NacosException exception throw.
     */
    public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag, String betaIps,
                                 String content, String encryptedDataKey, String casMd5, String type) throws NacosException {
        return agent.publishConfig(dataId, group, tenant, appName, tag, betaIps, content, encryptedDataKey, casMd5,
                type);
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            return cache;
        }

        String key = GroupKey.getKey(dataId, group);
        cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group);

        synchronized (cacheMap) {
            CacheData cacheFromMap = getCache(dataId, group);
            // multiple listeners on the same dataid+group and race condition,so double check again
            //other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                //reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                int taskId = calculateTaskId();
                increaseTaskIdCount(taskId);
                cache.setTaskId(taskId);
            }

            Map<String, CacheData> copy = new HashMap<>(cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }

        LOGGER.info("[{}] [subscribe] {}", this.agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        /**
         * 首次检查，获取特定dataId, group, tenant的缓存
         */
        CacheData cache = getCache(dataId, group, tenant);
        /**
         * 如果缓存已经存在，则直接返回；否则创建缓存
         */
        if (null != cache) {
            return cache;
        }
        /**
         * 使用特定dataId, group, tenant生成缓存key
         * key格式：${dataId}+${group}+${tenant} 或 ${dataId}+${group}
         */
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        synchronized (cacheMap) {
            /**
             * 第二次检查
             */
            CacheData cacheFromMap = getCache(dataId, group, tenant);
            // multiple listeners on the same dataid+group and race condition,so
            // double check again
            // other listener thread beat me to set to cacheMap
            if (null != cacheFromMap) {
                cache = cacheFromMap;
                // reset so that server not hang this check
                cache.setInitializing(true);
            } else {
                /**
                 * 创建保存配置的缓存，该缓存中除了配置内容以外，还加入了配置过滤责任链，确保缓存中返回的配置内容是已经经过处理的
                 */
                cache = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
                /**
                 * 生成任务id
                 */
                int taskId = calculateTaskId();
                /**
                 * 自增任务id
                 */
                increaseTaskIdCount(taskId);
                /**
                 * 给缓存设置任务id
                 */
                cache.setTaskId(taskId);
                // fix issue # 1317
                if (enableRemoteSyncConfig) {
                    /**
                     * 启用同步远程配置的开关后，在首次创建缓存时就获取配置内容，确保缓存内容不为空
                     */
                    ConfigResponse response = getServerConfig(dataId, group, tenant, 3000L, false);
                    cache.setEncryptedDataKey(response.getEncryptedDataKey());
                    cache.setContent(response.getContent());
                }
            }

            /**
             * 将创建好的缓存key和缓存对象保存到{@link cacheMap}
             */
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.get().size());

        return cache;
    }

    /**
     * Put cache.
     *
     * @param key   groupKey
     * @param cache cache
     */
    private void putCache(String key, CacheData cache) {
        synchronized (cacheMap) {
            Map<String, CacheData> copy = new HashMap<>(this.cacheMap.get());
            copy.put(key, cache);
            cacheMap.set(copy);
        }
    }

    private void increaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).incrementAndGet();
    }

    private void decreaseTaskIdCount(int taskId) {
        taskIdCacheCountList.get(taskId).decrementAndGet();
    }

    private int calculateTaskId() {
        int perTaskSize = (int) ParamUtil.getPerTaskConfigSize();
        for (int index = 0; index < taskIdCacheCountList.size(); index++) {
            if (taskIdCacheCountList.get(index).get() < perTaskSize) {
                return index;
            }
        }
        taskIdCacheCountList.add(new AtomicInteger(0));
        return taskIdCacheCountList.size() - 1;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    /**
     * 获取nacos服务器上特定dataId, group, tenant的配置
     *
     * @param dataId
     * @param group
     * @param tenant
     * @param readTimeout
     * @param notify
     * @return
     * @throws NacosException
     */
    public ConfigResponse getServerConfig(String dataId, String group, String tenant, long readTimeout, boolean notify)
            throws NacosException {
        /**
         * 如果分组未传递，则取 DEFAULT(DEFAULT_GROUP)
         */
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }
        /**
         * 使用RPC发起查询配置请求，并返回结果
         */
        return this.agent.queryConfig(dataId, group, tenant, readTimeout, notify);
    }

    private String blank2defaultGroup(String group) {
        return StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final ConfigFilterChainManager configFilterChainManager, ServerListManager serverListManager,
                        final NacosClientProperties properties) throws NacosException {
        this.configFilterChainManager = configFilterChainManager;
        /**
         * 从属性中取值初始化 {@link timeout,taskPenaltyTime,enableRemoteSyncConfig}
         */
        init(properties);

        /**
         * 实例化用于配置的Rpc传输客户端
         */
        agent = new ConfigRpcTransportClient(properties, serverListManager);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(initWorkerThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.Worker"));
        agent.setExecutor(executorService);
        /**
         * 启动Rpc传输客户端
         */
        agent.start();

    }

    void initAppLabels(Properties properties) {
        this.appLables = ConnLabelsUtils.addPrefixForEachKey(defaultLabelsCollectorManager.getLabels(properties),
                APP_CONN_PREFIX);
    }

    private int initWorkerThreadCount(NacosClientProperties properties) {
        /**
         * 获取线程数，该线程数是大于处理器数量的最小2的倍数，例如10个处理器，那么线程数就是16
         */
        int count = ThreadUtils.getSuitableThreadCount(THREAD_MULTIPLE);
        if (properties == null) {
            return count;
        }
        /**
         * 从 PROPERTIES(clientWorkerMaxThreadCount) | count 取较小值
         */
        count = Math.min(count, properties.getInteger(PropertyKeyConst.CLIENT_WORKER_MAX_THREAD_COUNT, count));
        /**
         * 从 count | DEFAULT(2) 取较小值
         */
        count = Math.max(count, MIN_THREAD_NUM);
        /**
         * 从 PROPERTIES(clientWorkerThreadCount) -> count 取值
         */
        return properties.getInteger(PropertyKeyConst.CLIENT_WORKER_THREAD_COUNT, count);
    }

    private void init(NacosClientProperties properties) {
        /**
         * 解析超时时间，从 PROPERTIES(configLongPollTimeout) -> DEFAULT(30000) | DEFAULT(10000) 取最大值
         */
        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
                Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);

        /**
         * 解析任务重试时间，从 PROPERTIES(configRetryTime) -> DEFAULT(2000) 取值
         */
        taskPenaltyTime = ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME),
                Constants.CONFIG_RETRY_TIME);

        /**
         * 解析是否启用远程同步配置开关，从 PROPERTIES(enableRemoteSyncConfig) 取值
         */
        this.enableRemoteSyncConfig = Boolean.parseBoolean(
                properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));

        /**
         * 解析应用标签列表
         */
        initAppLabels(properties.getProperties(SourceType.PROPERTIES));
    }

    Map<String, Object> getMetrics(List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        Map<String, Object> metric = new HashMap<>(16);
        metric.put("listenConfigSize", String.valueOf(this.cacheMap.get().size()));
        metric.put("clientVersion", VersionUtils.getFullClientVersion());
        metric.put("snapshotDir", LocalConfigInfoProcessor.LOCAL_SNAPSHOT_PATH);
        boolean isFixServer = agent.serverListManager.isFixed;
        metric.put("isFixedServer", isFixServer);
        metric.put("addressUrl", agent.serverListManager.addressServerUrl);
        metric.put("serverUrls", agent.serverListManager.getUrlString());

        Map<ClientConfigMetricRequest.MetricsKey, Object> metricValues = getMetricsValue(metricsKeys);
        metric.put("metricValues", metricValues);
        Map<String, Object> metrics = new HashMap<>(1);
        metrics.put(uuid, JacksonUtils.toJson(metric));
        return metrics;
    }

    private Map<ClientConfigMetricRequest.MetricsKey, Object> getMetricsValue(
            List<ClientConfigMetricRequest.MetricsKey> metricsKeys) {
        if (metricsKeys == null) {
            return null;
        }
        Map<ClientConfigMetricRequest.MetricsKey, Object> values = new HashMap<>(16);
        for (ClientConfigMetricRequest.MetricsKey metricsKey : metricsKeys) {
            if (ClientConfigMetricRequest.MetricsKey.CACHE_DATA.equals(metricsKey.getType())) {
                CacheData cacheData = cacheMap.get().get(metricsKey.getKey());
                values.putIfAbsent(metricsKey,
                        cacheData == null ? null : cacheData.getContent() + ":" + cacheData.getMd5());
            }
            if (ClientConfigMetricRequest.MetricsKey.SNAPSHOT_DATA.equals(metricsKey.getType())) {
                String[] configStr = GroupKey.parseKey(metricsKey.getKey());
                String snapshot = LocalConfigInfoProcessor.getSnapshot(this.agent.getName(), configStr[0], configStr[1],
                        configStr[2]);
                values.putIfAbsent(metricsKey,
                        snapshot == null ? null : snapshot + ":" + MD5Utils.md5Hex(snapshot, ENCODE));
            }
        }
        return values;
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        if (agent != null) {
            agent.shutdown();
        }
        LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * check if it has any connectable server endpoint.
     *
     * @return true: that means has atleast one connected rpc client. flase: that means does not have any connected rpc
     * client.
     */
    public boolean isHealthServer() {
        return agent.isHealthServer();
    }

    public class ConfigRpcTransportClient extends ConfigTransportClient {
        /**
         * 多任务执行器，用于执行缓存的任务
         */
        Map<String, ExecutorService> multiTaskExecutor = new HashMap<>();

        /**
         * 任务队列，该任务队列的任务对象没有任何业务含义，仅用于触发消费端任务执行
         */
        private final BlockingQueue<Object> listenExecutebell = new ArrayBlockingQueue<>(1);

        private final Object bellItem = new Object();

        private long lastAllSyncTime = System.currentTimeMillis();

        Subscriber subscriber = null;

        /**
         * 3 minutes to check all listen cache keys.
         * 如果上次同步和本地开始缓存的间隔时间>=3minutes时，就代表需要同步更新所有的缓存
         */
        private static final long ALL_SYNC_INTERNAL = 3 * 60 * 1000L;

        public ConfigRpcTransportClient(NacosClientProperties properties, ServerListManager serverListManager) {
            super(properties, serverListManager);
        }

        private ConnectionType getConnectionType() {
            return ConnectionType.GRPC;
        }

        @Override
        public void shutdown() throws NacosException {
            super.shutdown();
            synchronized (RpcClientFactory.getAllClientEntries()) {
                LOGGER.info("Trying to shutdown transport client {}", this);
                Set<Map.Entry<String, RpcClient>> allClientEntries = RpcClientFactory.getAllClientEntries();
                Iterator<Map.Entry<String, RpcClient>> iterator = allClientEntries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, RpcClient> entry = iterator.next();
                    if (entry.getKey().startsWith(uuid)) {
                        LOGGER.info("Trying to shutdown rpc client {}", entry.getKey());

                        try {
                            entry.getValue().shutdown();
                        } catch (NacosException nacosException) {
                            nacosException.printStackTrace();
                        }
                        LOGGER.info("Remove rpc client {}", entry.getKey());
                        iterator.remove();
                    }
                }

                LOGGER.info("Shutdown executor {}", executor);
                executor.shutdown();
                Map<String, CacheData> stringCacheDataMap = cacheMap.get();
                for (Map.Entry<String, CacheData> entry : stringCacheDataMap.entrySet()) {
                    entry.getValue().setConsistentWithServer(false);
                }
                if (subscriber != null) {
                    NotifyCenter.deregisterSubscriber(subscriber);
                }
            }

        }

        private Map<String, String> getLabels() {

            Map<String, String> labels = new HashMap<>(2, 1);
            labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
            labels.put(RemoteConstants.LABEL_MODULE, RemoteConstants.LABEL_MODULE_CONFIG);
            labels.put(Constants.APPNAME, AppNameUtils.getAppName());
            if (EnvUtil.getSelfVipserverTag() != null) {
                labels.put(Constants.VIPSERVER_TAG, EnvUtil.getSelfVipserverTag());
            }
            if (EnvUtil.getSelfAmoryTag() != null) {
                labels.put(Constants.AMORY_TAG, EnvUtil.getSelfAmoryTag());
            }
            if (EnvUtil.getSelfLocationTag() != null) {
                labels.put(Constants.LOCATION_TAG, EnvUtil.getSelfLocationTag());
            }

            labels.putAll(appLables);
            return labels;
        }

        /**
         * 处理配置变更通知请求
         *
         * @param configChangeNotifyRequest
         * @param clientName
         * @return
         */
        ConfigChangeNotifyResponse handleConfigChangeNotifyRequest(ConfigChangeNotifyRequest configChangeNotifyRequest,
                                                                   String clientName) {
            /**
             * 输出服务端推送日志
             */
            LOGGER.info("[{}] [server-push] config changed. dataId={}, group={},tenant={}", clientName,
                    configChangeNotifyRequest.getDataId(), configChangeNotifyRequest.getGroup(),
                    configChangeNotifyRequest.getTenant());
            /**
             * ${dataId}+${group}+${tenant}
             */
            String groupKey = GroupKey.getKeyTenant(configChangeNotifyRequest.getDataId(),
                    configChangeNotifyRequest.getGroup(), configChangeNotifyRequest.getTenant());

            /**
             * 从缓存中读取数据，如果缓存中存在，则更新{@link CacheData#receiveNotifyChanged}和{@link CacheData#isConsistentWithServer}
             */
            CacheData cacheData = cacheMap.get().get(groupKey);
            if (cacheData != null) {
                synchronized (cacheData) {
                    cacheData.getReceiveNotifyChanged().set(true);
                    cacheData.setConsistentWithServer(false);
                    notifyListenConfig();
                }

            }
            /**
             * 返回响应对象
             */
            return new ConfigChangeNotifyResponse();
        }

        ClientConfigMetricResponse handleClientMetricsRequest(ClientConfigMetricRequest configMetricRequest) {
            ClientConfigMetricResponse response = new ClientConfigMetricResponse();
            /**
             *
             */
            response.setMetrics(getMetrics(configMetricRequest.getMetricsKeys()));
            return response;
        }

        private void initRpcClientHandler(final RpcClient rpcClientInner) {
            /*
             * Register Config Change /Config ReSync Handler
             */
            /**
             * 注册 配置变更通知请求
             */
            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                if (request instanceof ConfigChangeNotifyRequest) {
                    return handleConfigChangeNotifyRequest((ConfigChangeNotifyRequest) request,
                            rpcClientInner.getName());
                }
                return null;
            });

            /**
             * 注册 配置指标请求
             */
            rpcClientInner.registerServerRequestHandler((request, connection) -> {
                if (request instanceof ClientConfigMetricRequest) {
                    return handleClientMetricsRequest((ClientConfigMetricRequest) request);
                }
                return null;
            });

            rpcClientInner.registerConnectionListener(new ConnectionEventListener() {

                @Override
                public void onConnected(Connection connection) {
                    LOGGER.info("[{}] Connected,notify listen context...", rpcClientInner.getName());
                    notifyListenConfig();
                }

                @Override
                public void onDisConnect(Connection connection) {
                    String taskId = rpcClientInner.getLabels().get("taskId");
                    LOGGER.info("[{}] DisConnected,clear listen context...", rpcClientInner.getName());
                    Collection<CacheData> values = cacheMap.get().values();

                    for (CacheData cacheData : values) {
                        if (StringUtils.isNotBlank(taskId)) {
                            if (Integer.valueOf(taskId).equals(cacheData.getTaskId())) {
                                cacheData.setConsistentWithServer(false);
                            }
                        } else {
                            cacheData.setConsistentWithServer(false);
                        }
                    }
                }

            });

            rpcClientInner.serverListFactory(new ServerListFactory() {
                @Override
                public String genNextServer() {
                    return ConfigRpcTransportClient.super.serverListManager.getNextServerAddr();

                }

                @Override
                public String getCurrentServer() {
                    return ConfigRpcTransportClient.super.serverListManager.getCurrentServerAddr();

                }

                @Override
                public List<String> getServerList() {
                    return ConfigRpcTransportClient.super.serverListManager.getServerUrls();

                }
            });

            /**
             * 订阅{@link ServerListChangeEvent}事件
             */
            subscriber = new Subscriber() {
                @Override
                public void onEvent(Event event) {
                    rpcClientInner.onServerListChange();
                }

                @Override
                public Class<? extends Event> subscribeType() {
                    return ServerListChangeEvent.class;
                }
            };
            /**
             * 注册监听器
             */
            NotifyCenter.registerSubscriber(subscriber);
        }

        @Override
        public void startInternal() {
            /**
             * 设置定时调度任务，在后台每隔5L刷新缓存，并且该任务还可以通过任务队列直接触发，不必强制等待5s
             */
            executor.schedule(() -> {
                /**
                 * 当调度服务未终止时，一直循环
                 */
                while (!executor.isShutdown() && !executor.isTerminated()) {
                    try {
                        /**
                         * 等待5s
                         */
                        listenExecutebell.poll(5L, TimeUnit.SECONDS);
                        /**
                         * 任务执行器状态检查
                         */
                        if (executor.isShutdown() || executor.isTerminated()) {
                            continue;
                        }
                        /**
                         * 从服务端获取配置，刷新缓存，通知监听器
                         */
                        executeConfigListen();
                    } catch (Throwable e) {
                        LOGGER.error("[rpc listen execute] [rpc listen] exception", e);
                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException interruptedException) {
                            //ignore
                        }
                        notifyListenConfig();
                    }
                }
            }, 0L, TimeUnit.MILLISECONDS);

        }

        @Override
        public String getName() {
            return serverListManager.getName();
        }

        @Override
        public void notifyListenConfig() {
            listenExecutebell.offer(bellItem);
        }

        /**
         * 客户端获取正在监听、要移除的监听器，通知服务端。
         * 从服务端获取配置，刷新缓存，通知监听器。
         *
         * @throws NacosException
         */
        @Override
        public void executeConfigListen() throws NacosException {
            /**
             * 保存了监听中的缓存，Map<任务Id, 相同任务Id的缓存列表>
             */
            Map<String, List<CacheData>> listenCachesMap = new HashMap<>(16);
            /**
             * 保存了将要移除缓存，Map<任务Id, 相同惹怒我id的缓存列表>
             */
            Map<String, List<CacheData>> removeListenCachesMap = new HashMap<>(16);
            /**
             * 开始时间
             */
            long now = System.currentTimeMillis();
            /**
             * 检查距离上次同步的时间间隔，如果>=3minutes，则表示需要同步全部的缓存
             */
            boolean needAllSync = now - lastAllSyncTime >= ALL_SYNC_INTERNAL;
            /**
             * 依次取出缓存
             */
            for (CacheData cache : cacheMap.get().values()) {

                synchronized (cache) {
                    /**
                     * 检查位于本地的配置故障转移文件，如果配置存在，则加载到内存中；否则设置为从服务端读取
                     */
                    checkLocalConfig(cache);

                    /**
                     * 检查缓存是否要和服务端保持一致，如果需要保持一致，其会拉取服务端的配置，并比较配置的md5，
                     * 如果不一致，则通知到缓存上的监听器
                     */
                    if (cache.isConsistentWithServer()) {
                        cache.checkListenerMd5();
                        /**
                         * 判断是否需要全量同步，如果不是全量同步，就代表该缓存是否要移除此刻不需要通知到服务端
                         */
                        if (!needAllSync) {
                            continue;
                        }
                    }

                    /**
                     * 如果配置了使用本地缓存，则不再请求服务端
                     */
                    if (cache.isUseLocalConfigInfo()) {
                        continue;
                    }

                    /**
                     * 如果缓存设置了丢弃，则将其加入到{@link removeListenCachesMap}；反之加入到{@link listenCachesMap}。
                     */
                    if (!cache.isDiscard()) {
                        List<CacheData> cacheDatas = listenCachesMap.computeIfAbsent(String.valueOf(cache.getTaskId()), k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    } else {
                        List<CacheData> cacheDatas = removeListenCachesMap.computeIfAbsent(String.valueOf(cache.getTaskId()), k -> new LinkedList<>());
                        cacheDatas.add(cache);
                    }
                }

            }

            /**
             * 检查本地缓存和服务端配置是否相等
             */
            boolean hasChangedKeys = checkListenCache(listenCachesMap);

            //execute check remove listen.
            /**
             * 通知服务端移除不需要监听的监听器
             */
            checkRemoveListenCache(removeListenCachesMap);

            /**
             * 如果本次是全量同步，则更新{@link #lastAllSyncTime}
             */
            if (needAllSync) {
                lastAllSyncTime = now;
            }
            //If has changed keys,notify re sync md5.
            /**
             * 如果发生配置变更，则再次触发{@link #executeConfigListen()}
             */
            if (hasChangedKeys) {
                notifyListenConfig();
            }
        }

        /**
         * 校验本地配置文件，如果本地存在故障转移文件，则将该文件加载到缓存中；否则设置缓存从服务端读取配置
         *
         * @param cacheData The CacheData object to be processed.
         */
        public void checkLocalConfig(CacheData cacheData) {
            final String dataId = cacheData.dataId;
            final String group = cacheData.group;
            final String tenant = cacheData.tenant;
            final String envName = cacheData.envName;

            /**
             * 检查特定的dataId, group, tenant的故障恢复文件是否存在
             */
            File file = LocalConfigInfoProcessor.getFailoverFile(envName, dataId, group, tenant);

            /**
             * 如果配置了不适用本地配置，但是该文件存在，即代表应该使用本地配置
             */
            if (!cacheData.isUseLocalConfigInfo() && file.exists()) {
                /**
                 * 从本地配置读取配置内容
                 */
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                /**
                 * 计算改内容的MD5，用于日志输出
                 */
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                /**
                 * 修正为使用本地配置
                 */
                cacheData.setUseLocalConfigInfo(true);
                /**
                 * 设置配置上次编辑的时间
                 */
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                /**
                 * 将故障转移文件中的配置内容写入缓存
                 */
                cacheData.setContent(content);
                LOGGER.warn(
                        "[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                        envName, dataId, group, tenant, md5, ContentUtils.truncateContent(content));
                return;
            }

            /**
             * 如果使用本地配置，但是故障恢复文件不存在，则切换为使用服务端文件，直接返回
             */
            if (cacheData.isUseLocalConfigInfo() && !file.exists()) {
                /**
                 * 尽管设置了使用本地缓存，但是并未设置对应的配置文件，则表示为实际上不需要使用本地缓存。
                 * 此时对本地缓存配置进行修正，即不需要使用本地缓存
                 */
                cacheData.setUseLocalConfigInfo(false);
                LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", envName, dataId, group, tenant);
                return;
            }

            /**
             * 当故障恢复文件发生变更，从故障恢复中读取内容，更新缓存
             */
            if (cacheData.isUseLocalConfigInfo() && file.exists() && cacheData.getLocalConfigInfoVersion() != file.lastModified()) {
                String content = LocalConfigInfoProcessor.getFailover(envName, dataId, group, tenant);
                final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                cacheData.setUseLocalConfigInfo(true);
                cacheData.setLocalConfigInfoVersion(file.lastModified());
                cacheData.setContent(content);
                LOGGER.warn(
                        "[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                        envName, dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            }
        }

        private ExecutorService ensureSyncExecutor(String taskId) {
            /**
             * 如果多任务执行器不存在指定任务ID的执行器，则创建对应的任务执行器
             */
            if (!multiTaskExecutor.containsKey(taskId)) {
                multiTaskExecutor.put(taskId,
                        /**
                         * 创建只有一个线程的，永远存活的，无限队列的任务执行器
                         */
                        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                            Thread thread = new Thread(r, "nacos.client.config.listener.task-" + taskId);
                            thread.setDaemon(true);
                            return thread;
                        }));
            }
            return multiTaskExecutor.get(taskId);
        }

        private void refreshContentAndCheck(RpcClient rpcClient, String groupKey, boolean notify) {
            /**
             * 如果在缓存映射中存在特定key的缓存，则刷新缓存内容，并通知缓存上的监听器
             */
            if (cacheMap.get() != null && cacheMap.get().containsKey(groupKey)) {
                CacheData cache = cacheMap.get().get(groupKey);
                refreshContentAndCheck(rpcClient, cache, notify);
            }
        }

        /**
         * 刷新缓存中特定dataId, group, tenant的配置内容，并通知缓存上注册的监听器
         *
         * @param rpcClient
         * @param cacheData
         * @param notify
         */
        private void refreshContentAndCheck(RpcClient rpcClient, CacheData cacheData, boolean notify) {
            try {
                /**
                 * RPC请求服务端，超时时间3000L
                 */
                ConfigResponse response = this.queryConfigInner(rpcClient, cacheData.dataId, cacheData.group,
                        cacheData.tenant, 3000L, notify);
                /**
                 * 刷新缓存中的密钥、配置内容、配置格式
                 */
                cacheData.setEncryptedDataKey(response.getEncryptedDataKey());
                cacheData.setContent(response.getContent());
                if (null != response.getConfigType()) {
                    cacheData.setType(response.getConfigType());
                }
                /**
                 * 如果开启通知，就输出本地获取到的配置内容到日志中
                 */
                if (notify) {
                    LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                            agent.getName(), cacheData.dataId, cacheData.group, cacheData.tenant, cacheData.getMd5(),
                            ContentUtils.truncateContent(response.getContent()), response.getConfigType());
                }
                /**
                 * 当缓存内容刷新时，通知到所有的监听器
                 */
                cacheData.checkListenerMd5();
            } catch (Exception e) {
                LOGGER.error("refresh content and check md5 fail ,dataId={},group={},tenant={} ", cacheData.dataId,
                        cacheData.group, cacheData.tenant, e);
            }
        }

        /**
         * 校验并移除指定缓存的监听器，并通知到服务端
         *
         * @param removeListenCachesMap
         * @throws NacosException
         */
        private void checkRemoveListenCache(Map<String, List<CacheData>> removeListenCachesMap) throws NacosException {
            if (!removeListenCachesMap.isEmpty()) {
                List<Future> listenFutures = new ArrayList<>();

                for (Map.Entry<String, List<CacheData>> entry : removeListenCachesMap.entrySet()) {
                    /**
                     * 获取任务id
                     */
                    String taskId = entry.getKey();
                    /**
                     * 根据任务id获取{@link RpcClient}
                     */
                    RpcClient rpcClient = ensureRpcClient(taskId);
                    /**
                     * 根据任务id获取{@link ExecutorService}
                     */
                    ExecutorService executorService = ensureSyncExecutor(taskId);

                    Future future = executorService.submit(() -> {
                        List<CacheData> removeListenCaches = entry.getValue();
                        /**
                         * 将缓存列表转换为{@link ConfigBatchListenRequest}，
                         * 并设置{@link ConfigBatchListenRequest#setListen(false)}，代表撤销服务端的监听器
                         */
                        ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(removeListenCaches);
                        configChangeListenRequest.setListen(false);
                        try {
                            /**
                             * 调用RPC请求，通知服务端清除对应的
                             */
                            boolean removeSuccess = unListenConfigChange(rpcClient, configChangeListenRequest);
                            /**
                             * 如果请求成功，则同步清除本地
                             */
                            if (removeSuccess) {
                                for (CacheData cacheData : removeListenCaches) {
                                    synchronized (cacheData) {
                                        /**
                                         * 如果缓存是可丢弃的，并且缓存上也没有任何监听器，则从缓存映射中移除该缓存；
                                         * 反之，如果缓存上还有监听器，即使服务端清除了，但是本地仍然存在，那么在下次全量同步的时候，
                                         */
                                        if (cacheData.isDiscard() && cacheData.getListeners().isEmpty()) {
                                            ClientWorker.this.removeCache(cacheData.dataId, cacheData.group, cacheData.tenant);
                                        }
                                    }
                                }
                            }
                        } catch (Throwable e) {
                            LOGGER.error("Async remove listen config change error ", e);
                            try {
                                Thread.sleep(50L);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                            notifyListenConfig();
                        }
                    });
                    listenFutures.add(future);

                }
                for (Future future : listenFutures) {
                    try {
                        future.get();
                    } catch (Throwable throwable) {
                        LOGGER.error("Async remove listen config change error ", throwable);
                    }
                }
            }
        }

        /**
         * 校验本地缓存和服务端配置是否一致，一致性返回true
         *
         * @param listenCachesMap
         * @return
         * @throws NacosException
         */
        private boolean checkListenCache(Map<String, List<CacheData>> listenCachesMap) throws NacosException {
            if (listenCachesMap.isEmpty()) {
                return false;
            }

            final AtomicBoolean hasChangedKeys = new AtomicBoolean(false);
            /**
             * 对于不同的taskId，会被封装为不同的任务，并由Future持有其引用
             */
            List<Future> listenFutures = new ArrayList<>();

            /**
             * 依次取出缓存列表
             */
            for (Map.Entry<String, List<CacheData>> entry : listenCachesMap.entrySet()) {
                String taskId = entry.getKey();
                RpcClient rpcClient = ensureRpcClient(taskId);
                /**
                 * 获取处理这些缓存的任务执行器
                 */
                ExecutorService executorService = ensureSyncExecutor(taskId);

                /**
                 * 向任务执行器中提交任务，
                 */
                Future future = executorService.submit(() -> {
                    /**
                     * 获取这次任务要处理的缓存
                     */
                    List<CacheData> listenCaches = entry.getValue();
                    /**
                     * 重置通知变更标识位
                     */
                    for (CacheData cacheData : listenCaches) {
                        cacheData.getReceiveNotifyChanged().set(false);
                    }
                    /**
                     * 构造批次处理监听器请求
                     */
                    ConfigBatchListenRequest configChangeListenRequest = buildConfigRequest(listenCaches);
                    /**
                     * 设置为监听
                     */
                    configChangeListenRequest.setListen(true);
                    try {
                        ConfigChangeBatchListenResponse listenResponse = (ConfigChangeBatchListenResponse) requestProxy(
                                rpcClient, configChangeListenRequest);
                        if (listenResponse != null && listenResponse.isSuccess()) {

                            Set<String> changeKeys = new HashSet<String>();

                            List<ConfigChangeBatchListenResponse.ConfigContext> changedConfigs = listenResponse.getChangedConfigs();
                            //handle changed keys,notify listener
                            if (!CollectionUtils.isEmpty(changedConfigs)) {
                                hasChangedKeys.set(true);
                                for (ConfigChangeBatchListenResponse.ConfigContext changeConfig : changedConfigs) {
                                    String changeKey = GroupKey.getKeyTenant(changeConfig.getDataId(),
                                            changeConfig.getGroup(), changeConfig.getTenant());
                                    changeKeys.add(changeKey);
                                    boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                    refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                }

                            }

                            for (CacheData cacheData : listenCaches) {
                                if (cacheData.getReceiveNotifyChanged().get()) {
                                    String changeKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                            cacheData.getTenant());
                                    if (!changeKeys.contains(changeKey)) {
                                        boolean isInitializing = cacheMap.get().get(changeKey).isInitializing();
                                        refreshContentAndCheck(rpcClient, changeKey, !isInitializing);
                                    }
                                }
                            }

                            //handler content configs
                            for (CacheData cacheData : listenCaches) {
                                cacheData.setInitializing(false);
                                String groupKey = GroupKey.getKeyTenant(cacheData.dataId, cacheData.group,
                                        cacheData.getTenant());
                                if (!changeKeys.contains(groupKey)) {
                                    synchronized (cacheData) {
                                        if (!cacheData.getReceiveNotifyChanged().get()) {
                                            cacheData.setConsistentWithServer(true);
                                        }
                                    }
                                }
                            }

                        }
                    } catch (Throwable e) {
                        LOGGER.error("Execute listen config change error ", e);
                        try {
                            Thread.sleep(50L);
                        } catch (InterruptedException interruptedException) {
                            //ignore
                        }
                        notifyListenConfig();
                    }
                });
                listenFutures.add(future);
            }

            /**
             * 确保所有任务都处理结束
             */
            for (Future future : listenFutures) {
                try {
                    future.get();
                } catch (Throwable throwable) {
                    LOGGER.error("Async listen config change error ", throwable);
                }
            }
            return hasChangedKeys.get();
        }

        private RpcClient ensureRpcClient(String taskId) throws NacosException {
            synchronized (ClientWorker.this) {
                Map<String, String> labels = getLabels();
                Map<String, String> newLabels = new HashMap<>(labels);
                newLabels.put("taskId", taskId);
                /**
                 * 创建RPC客户端TLS配置
                 */
                RpcClientTlsConfig clientTlsConfig = RpcClientTlsConfigFactory.getInstance().createSdkConfig(properties);
                /**
                 * 创建RPC客户端
                 */
                RpcClient rpcClient = RpcClientFactory.createClient(uuid + "_config-" + taskId, getConnectionType(), newLabels, clientTlsConfig);
                /**
                 * rpc客户端还未初始化，则执行初始化
                 */
                if (rpcClient.isWaitInitiated()) {
                    initRpcClientHandler(rpcClient);
                    rpcClient.setTenant(getTenant());
                    rpcClient.start();
                }

                return rpcClient;
            }

        }

        /**
         * build config string.
         *
         * @param caches caches to build config string.
         * @return request.
         */
        private ConfigBatchListenRequest buildConfigRequest(List<CacheData> caches) {

            ConfigBatchListenRequest configChangeListenRequest = new ConfigBatchListenRequest();
            for (CacheData cacheData : caches) {
                configChangeListenRequest.addConfigListenContext(cacheData.group, cacheData.dataId, cacheData.tenant,
                        cacheData.getMd5());
            }
            return configChangeListenRequest;
        }

        @Override
        public void removeCache(String dataId, String group) {
            // Notify to rpc un listen ,and remove cache if success.
            notifyListenConfig();
        }

        /**
         * 发送取消监听配置变更请求，该方法仅直接调用目标方法，因此需要在传递过来的{@link ConfigBatchListenRequest#setListen(false)}
         *
         * @param configChangeListenRequest request of remove listen config string.
         */
        private boolean unListenConfigChange(RpcClient rpcClient, ConfigBatchListenRequest configChangeListenRequest)
                throws NacosException {

            ConfigChangeBatchListenResponse response = (ConfigChangeBatchListenResponse) requestProxy(rpcClient, configChangeListenRequest);
            return response.isSuccess();
        }

        @Override
        public ConfigResponse queryConfig(String dataId, String group, String tenant, long readTimeouts, boolean notify)
                throws NacosException {
            RpcClient rpcClient = getOneRunningClient();
            if (notify) {
                CacheData cacheData = cacheMap.get().get(GroupKey.getKeyTenant(dataId, group, tenant));
                if (cacheData != null) {
                    rpcClient = ensureRpcClient(String.valueOf(cacheData.getTaskId()));
                }
            }

            /**
             * 使用grpc请求服务端获取配置
             */
            return queryConfigInner(rpcClient, dataId, group, tenant, readTimeouts, notify);

        }

        /**
         * 使用RPC客户端从服务端查询特定dataId, group, tenant的配置
         *
         * @param rpcClient
         * @param dataId
         * @param group
         * @param tenant
         * @param readTimeouts
         * @param notify
         * @return
         * @throws NacosException
         */
        ConfigResponse queryConfigInner(RpcClient rpcClient, String dataId, String group, String tenant,
                                        long readTimeouts, boolean notify) throws NacosException {
            /**
             * 构造配置查询请求
             */
            ConfigQueryRequest request = ConfigQueryRequest.build(dataId, group, tenant);
            /**
             * 设置请求头notify
             */
            request.putHeader(NOTIFY_HEADER, String.valueOf(notify));

            /**
             * 执行RPC请求
             */
            ConfigQueryResponse response = (ConfigQueryResponse) requestProxy(rpcClient, request, readTimeouts);

            ConfigResponse configResponse = new ConfigResponse();
            if (response.isSuccess()) {
                /**
                 * 请求成功
                 * 将结果保存到本地快照目录中
                 */
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, response.getContent());
                /**
                 * 回写配置内容
                 */
                configResponse.setContent(response.getContent());
                /**
                 * 如果内容类型未指定，则使用TEXT
                 */
                String configType;
                if (StringUtils.isNotBlank(response.getContentType())) {
                    configType = response.getContentType();
                } else {
                    configType = ConfigType.TEXT.getType();
                }
                /**
                 * 回写内容类型
                 */
                configResponse.setConfigType(configType);
                String encryptedDataKey = response.getEncryptedDataKey();
                /**
                 * 将加密数据密钥写入本地文件
                 */
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, encryptedDataKey);
                /**
                 * 回写加密数据密钥
                 */
                configResponse.setEncryptedDataKey(encryptedDataKey);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_NOT_FOUND) {
                /**
                 * 未找到配置，将空内容写入本地快照目录中
                 */
                LocalConfigInfoProcessor.saveSnapshot(this.getName(), dataId, group, tenant, null);
                /**
                 * 将空加密数据密钥写入本地文件
                 */
                LocalEncryptedDataKeyProcessor.saveEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant, null);
                return configResponse;
            } else if (response.getErrorCode() == ConfigQueryResponse.CONFIG_QUERY_CONFLICT) {
                /**
                 * 配置正在被编辑
                 */
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", this.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            } else {
                /**
                 * 未知错误
                 */
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", this.getName(), dataId,
                        group, tenant, response);
                throw new NacosException(response.getErrorCode(),
                        "http error, code=" + response.getErrorCode() + ",msg=" + response.getMessage() + ",dataId="
                                + dataId + ",group=" + group + ",tenant=" + tenant);
            }
        }

        private Response requestProxy(RpcClient rpcClientInner, Request request) throws NacosException {
            return requestProxy(rpcClientInner, request, 3000L);
        }

        private Response requestProxy(RpcClient rpcClientInner, Request request, long timeoutMills)
                throws NacosException {
            // 设置请求头
            try {
                /**
                 * 设置安全代理请求头
                 */
                request.putAllHeader(super.getSecurityHeaders(resourceBuild(request)));
                /**
                 * 设置common头
                 */
                request.putAllHeader(super.getCommonHeader());
            } catch (Exception e) {
                throw new NacosException(NacosException.CLIENT_INVALID_PARAM, e);
            }

            /**
             * 从请求中移除headers和requestId
             */
            JsonObject asJsonObjectTemp = new Gson().toJsonTree(request).getAsJsonObject();
            asJsonObjectTemp.remove("headers");
            asJsonObjectTemp.remove("requestId");

            /**
             * 判断请求是否被限流
             */
            boolean limit = Limiter.isLimit(request.getClass() + asJsonObjectTemp.toString());
            if (limit) {
                throw new NacosException(NacosException.CLIENT_OVER_THRESHOLD,
                        "More than client-side current limit threshold");
            }
            /**
             * 执行请求
             */
            return rpcClientInner.request(request, timeoutMills);
        }

        private RequestResource resourceBuild(Request request) {
            if (request instanceof ConfigQueryRequest) {
                String tenant = ((ConfigQueryRequest) request).getTenant();
                String group = ((ConfigQueryRequest) request).getGroup();
                String dataId = ((ConfigQueryRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            if (request instanceof ConfigPublishRequest) {
                String tenant = ((ConfigPublishRequest) request).getTenant();
                String group = ((ConfigPublishRequest) request).getGroup();
                String dataId = ((ConfigPublishRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }

            if (request instanceof ConfigRemoveRequest) {
                String tenant = ((ConfigRemoveRequest) request).getTenant();
                String group = ((ConfigRemoveRequest) request).getGroup();
                String dataId = ((ConfigRemoveRequest) request).getDataId();
                return buildResource(tenant, group, dataId);
            }
            return RequestResource.configBuilder().build();
        }

        RpcClient getOneRunningClient() throws NacosException {
            return ensureRpcClient("0");
        }

        @Override
        public boolean publishConfig(String dataId, String group, String tenant, String appName, String tag,
                                     String betaIps, String content, String encryptedDataKey, String casMd5, String type)
                throws NacosException {
            try {
                ConfigPublishRequest request = new ConfigPublishRequest(dataId, group, tenant, content);
                request.setCasMd5(casMd5);
                request.putAdditionalParam(TAG_PARAM, tag);
                request.putAdditionalParam(APP_NAME_PARAM, appName);
                request.putAdditionalParam(BETAIPS_PARAM, betaIps);
                request.putAdditionalParam(TYPE_PARAM, type);
                request.putAdditionalParam(ENCRYPTED_DATA_KEY_PARAM, encryptedDataKey == null ? "" : encryptedDataKey);
                ConfigPublishResponse response = (ConfigPublishResponse) requestProxy(getOneRunningClient(), request);
                if (!response.isSuccess()) {
                    LOGGER.warn("[{}] [publish-single] fail, dataId={}, group={}, tenant={}, code={}, msg={}",
                            this.getName(), dataId, group, tenant, response.getErrorCode(), response.getMessage());
                    return false;
                } else {
                    LOGGER.info("[{}] [publish-single] ok, dataId={}, group={}, tenant={}, config={}", getName(),
                            dataId, group, tenant, ContentUtils.truncateContent(content));
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}",
                        this.getName(), dataId, group, tenant, "unknown", e.getMessage());
                return false;
            }
        }

        @Override
        public boolean removeConfig(String dataId, String group, String tenant, String tag) throws NacosException {
            /**
             * 构造移除配置请求
             */
            ConfigRemoveRequest request = new ConfigRemoveRequest(dataId, group, tenant, tag);
            /**
             * 执行RPC请求，并获取响应，默认超时时间3000ms
             */
            ConfigRemoveResponse response = (ConfigRemoveResponse) requestProxy(getOneRunningClient(), request);
            /**
             * 读取响应是否成功
             */
            return response.isSuccess();
        }

        /**
         * check server is health.
         *
         * @return
         */
        public boolean isHealthServer() {
            try {
                return getOneRunningClient().isRunning();
            } catch (NacosException e) {
                LOGGER.warn("check server status failed.", e);
                return false;
            }
        }
    }

    public String getAgentName() {
        return this.agent.getName();
    }

    public ConfigTransportClient getAgent() {
        return this.agent;
    }

}
