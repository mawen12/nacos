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

import com.alibaba.nacos.api.config.filter.AbstractConfigFilter;
import com.alibaba.nacos.api.config.filter.IConfigFilterChain;
import com.alibaba.nacos.api.config.filter.IConfigRequest;
import com.alibaba.nacos.api.config.filter.IConfigResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.Pair;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.encryption.handler.EncryptionHandler;

import java.util.Objects;
import java.util.Properties;

/**
 * Configure encryption filter.
 *
 * 配置加密过滤器
 *
 * @author lixiaoshuang
 */
public class ConfigEncryptionFilter extends AbstractConfigFilter {
    
    private static final String DEFAULT_NAME = ConfigEncryptionFilter.class.getName();
    
    @Override
    public void init(Properties properties) {
    
    }
    
    @Override
    public void doFilter(IConfigRequest request, IConfigResponse response, IConfigFilterChain filterChain)
            throws NacosException {
        /**
         * 在请求阶段，仅对ConfigRequest进行过滤
         */
        if (Objects.nonNull(request) && request instanceof ConfigRequest && Objects.isNull(response)) {
            
            // Publish configuration, encrypt
            ConfigRequest configRequest = (ConfigRequest) request;
            String dataId = configRequest.getDataId();
            String content = configRequest.getContent();

            /**
             * 对dataId和content进行加密，并生成密钥和加密内容
             */
            Pair<String, String> pair = EncryptionHandler.encryptHandler(dataId, content);
            String secretKey = pair.getFirst();
            String encryptContent = pair.getSecond();
            /**
             * 用加密后的内容替换原先内容
             */
            if (!StringUtils.isBlank(encryptContent) && !encryptContent.equals(content)) {
                ((ConfigRequest) request).setContent(encryptContent);
            }
            /**
             * 如果当前密钥不为空，并且和当前不一致，则使用当前加密密钥覆盖之前的；
             * 如果之前并未设置，且当前密钥为空，则置为空字符串
             */
            if (!StringUtils.isBlank(secretKey) && !secretKey.equals(((ConfigRequest) request).getEncryptedDataKey())) {
                ((ConfigRequest) request).setEncryptedDataKey(secretKey);
            } else if (StringUtils.isBlank(((ConfigRequest) request).getEncryptedDataKey()) && StringUtils.isBlank(secretKey)) {
                ((ConfigRequest) request).setEncryptedDataKey("");
            }
        }

        /**
         * 在响应阶段，仅对ConfigResponse进行过滤，且请求为空
         */
        if (Objects.nonNull(response) && response instanceof ConfigResponse && Objects.isNull(request)) {
            
            // Get configuration, decrypt
            ConfigResponse configResponse = (ConfigResponse) response;
            /**
             * 从响应中解析dataId、密钥、加密内容
             */
            String dataId = configResponse.getDataId();
            String encryptedDataKey = configResponse.getEncryptedDataKey();
            String content = configResponse.getContent();
            /**
             * 对内容进行解密
             */
            Pair<String, String> pair = EncryptionHandler.decryptHandler(dataId, encryptedDataKey, content);
            String secretKey = pair.getFirst();
            String decryptContent = pair.getSecond();
            /**
             * 解密内容不为空，并且解密内容不等于当前内容，则覆盖之前的
             */
            if (!StringUtils.isBlank(decryptContent) && !decryptContent.equals(content)) {
                ((ConfigResponse) response).setContent(decryptContent);
            }
            /**
             * 如果当前密钥不为空，并且与加密密钥不相等，则覆盖之前的
             */
            if (!StringUtils.isBlank(secretKey) && !secretKey.equals(((ConfigResponse) response).getEncryptedDataKey())) {
                ((ConfigResponse) response).setEncryptedDataKey(secretKey);
            } else if (StringUtils.isBlank(((ConfigResponse) response).getEncryptedDataKey()) && StringUtils.isBlank(secretKey)) {
                ((ConfigResponse) response).setEncryptedDataKey("");
            }
        }
        /**
         * 链式过滤
         */
        filterChain.doFilter(request, response);
    }
    
    @Override
    public int getOrder() {
        return 0;
    }
    
    @Override
    public String getFilterName() {
        return DEFAULT_NAME;
    }
    
}
