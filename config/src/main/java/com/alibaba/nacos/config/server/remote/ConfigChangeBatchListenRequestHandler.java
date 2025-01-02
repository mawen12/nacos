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

package com.alibaba.nacos.config.server.remote;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest;
import com.alibaba.nacos.api.config.remote.response.ConfigChangeBatchListenResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.utils.GroupKey2;
import com.alibaba.nacos.core.paramcheck.ExtractorManager;
import com.alibaba.nacos.core.paramcheck.impl.ConfigBatchListenRequestParamExtractor;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.utils.StringPool;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import com.alibaba.nacos.plugin.auth.constant.SignType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * config change listen request handler.
 *
 * 匹配变更批量监听器
 *
 * @see {@link com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest}
 * @see {@link com.alibaba.nacos.api.config.remote.request.ConfigBatchListenRequest.ConfigListenContext}
 * @see {@link com.alibaba.nacos.client.config.impl.ClientWorker.ConfigRpcTransportClient}
 * @author liuzunfei
 * @version $Id: ConfigChangeListenRequestHandler.java, v 0.1 2020年07月14日 10:11 AM liuzunfei Exp $
 */
@Component
public class ConfigChangeBatchListenRequestHandler
        extends RequestHandler<ConfigBatchListenRequest, ConfigChangeBatchListenResponse> {
    
    @Autowired
    private ConfigChangeListenContext configChangeListenContext;
    
    @Override
    @TpsControl(pointName = "ConfigListen")
    @Secured(action = ActionTypes.READ, signType = SignType.CONFIG)
    @ExtractorManager.Extractor(rpcExtractor = ConfigBatchListenRequestParamExtractor.class)
    public ConfigChangeBatchListenResponse handle(ConfigBatchListenRequest configChangeListenRequest, RequestMeta meta)
            throws NacosException {
        /**
         * 从字符串缓存池中获取连接id
         */
        String connectionId = StringPool.get(meta.getConnectionId());
        /**
         * 读取请求头：Vipserver-Tag
         */
        String tag = configChangeListenRequest.getHeader(Constants.VIPSERVER_TAG);

        /**
         * 构造响应
         */
        ConfigChangeBatchListenResponse configChangeBatchListenResponse = new ConfigChangeBatchListenResponse();
        for (ConfigBatchListenRequest.ConfigListenContext listenContext : configChangeListenRequest.getConfigListenContexts()) {
            /**
             * 获取key：${dataId}+${group}+${tenant}
             */
            String groupKey = GroupKey2.getKey(listenContext.getDataId(), listenContext.getGroup(), listenContext.getTenant());
            /**
             * 从字符串缓存池中获取key
             */
            groupKey = StringPool.get(groupKey);

            /**
             * 从字符串缓存池中获取md5
             */
            String md5 = StringPool.get(listenContext.getMd5());
            /**
             * 如果是正在监听，则加入到配置变更监听上下文中；反之代表不再监听，比如在Nacos客户端移除了监听器
             */
            if (configChangeListenRequest.isListen()) {
                configChangeListenContext.addListen(groupKey, md5, connectionId);
                /**
                 * 从缓存中检查特定groupKey是否为最新的逻辑：
                 * 1.获取特定key的配置
                 * 2.比较md5值
                 */
                boolean isUptoDate = ConfigCacheService.isUptodate(groupKey, md5, meta.getClientIp(), tag);
                /**
                 * 如果不是最新的，就需要在响应中添加特定dataId, group, tenant，表示该配置已发生变更
                 */
                if (!isUptoDate) {
                    configChangeBatchListenResponse.addChangeConfig(listenContext.getDataId(), listenContext.getGroup(), listenContext.getTenant());
                }
            } else {
                /**
                 * 移除监听器
                 */
                configChangeListenContext.removeListen(groupKey, connectionId);
            }
        }
        
        return configChangeBatchListenResponse;
        
    }
    
}
