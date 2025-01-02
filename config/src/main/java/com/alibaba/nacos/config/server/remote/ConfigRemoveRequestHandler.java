/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

import com.alibaba.nacos.api.config.remote.request.ConfigRemoveRequest;
import com.alibaba.nacos.api.config.remote.response.ConfigRemoveResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.config.server.service.ConfigChangePublisher;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoTagPersistService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.core.paramcheck.ExtractorManager;
import com.alibaba.nacos.core.paramcheck.impl.ConfigRequestParamExtractor;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import com.alibaba.nacos.plugin.auth.constant.SignType;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

/**
 * 处理Nacos Client移除配置的入口，该方法是基于RPC的。
 *
 * @author liuzunfei
 * @version $Id: ConfiRemoveRequestHandler.java, v 0.1 2020年07月16日 5:49 PM liuzunfei Exp $
 * @see {@link com.alibaba.nacos.api.config.ConfigService#removeConfig(String, String)}
 */
@Component
public class ConfigRemoveRequestHandler extends RequestHandler<ConfigRemoveRequest, ConfigRemoveResponse> {

    private final ConfigInfoPersistService configInfoPersistService;

    private final ConfigInfoTagPersistService configInfoTagPersistService;

    public ConfigRemoveRequestHandler(ConfigInfoPersistService configInfoPersistService,
                                      ConfigInfoTagPersistService configInfoTagPersistService) {
        this.configInfoPersistService = configInfoPersistService;
        this.configInfoTagPersistService = configInfoTagPersistService;
    }

    @Override
    @TpsControl(pointName = "ConfigRemove")
    @Secured(action = ActionTypes.WRITE, signType = SignType.CONFIG)
    @ExtractorManager.Extractor(rpcExtractor = ConfigRequestParamExtractor.class)
    public ConfigRemoveResponse handle(ConfigRemoveRequest configRemoveRequest, RequestMeta meta)
            throws NacosException {
        /**
         * 读取请求参数
         */
        String tenant = configRemoveRequest.getTenant();
        String dataId = configRemoveRequest.getDataId();
        String group = configRemoveRequest.getGroup();
        String tag = configRemoveRequest.getTag();

        try {
            /**
             * 校验命名空间合法字符（[a-z][A-Z][0-9][-_.:]），长度<=128
             */
            ParamUtils.checkTenant(tenant);
            /**
             * 校验dataId合法字符([a-z][A-Z][0-9][-_.:])
             *    分组合法字符([a-z][A-Z][0-9][-_.:])
             */
            ParamUtils.checkParam(dataId, group, "datumId", "rm");
            /**
             * 校验标签的字符合法([a-z][A-Z][0-9][-_.:])，长度<=16
             */
            ParamUtils.checkParam(tag);
            String persistEvent = ConfigTraceService.PERSISTENCE_EVENT;

            String clientIp = meta.getClientIp();
            if (StringUtils.isBlank(tag)) {
                /**
                 * 如果未指定标签，则同时移除配置和其标签信息
                 */
                configInfoPersistService.removeConfigInfo(dataId, group, tenant, clientIp, null);
            } else {
                persistEvent = ConfigTraceService.PERSISTENCE_EVENT_TAG + "-" + tag;

                /**
                 * 如果指定标签，则仅移除标签
                 */
                configInfoTagPersistService.removeConfigInfoTag(dataId, group, tenant, tag, clientIp, null);
            }
            final Timestamp time = TimeUtils.getCurrentTime();
            ConfigTraceService.logPersistenceEvent(dataId, group, tenant, null, time.getTime(), clientIp, persistEvent, ConfigTraceService.PERSISTENCE_TYPE_REMOVE, null);
            ConfigChangePublisher.notifyConfigChange(new ConfigDataChangeEvent(false, dataId, group, tenant, tag, time.getTime()));
            return ConfigRemoveResponse.buildSuccessResponse();

        } catch (Exception e) {
            Loggers.REMOTE_DIGEST.error("remove config error,error msg is {}", e.getMessage(), e);
            return ConfigRemoveResponse.buildFailResponse(e.getMessage());
        }
    }

}
