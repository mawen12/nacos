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

package com.alibaba.nacos.client.naming.backups;

/**
 * 客户端的灾难恢复数据，虽然预制了两张类型，但是目前仅支持注册中心
 *
 * @author zongkang.guo
 */
public class FailoverData {
    
    /**
     * 灾难恢复类型，注册中心还是配置中心
     */
    private final DataType dataType;
    
    /**
     * 灾难恢复数据，对于{@link DataType#naming}来说，就是{@link com.alibaba.nacos.api.naming.pojo.ServiceInfo}
     */
    private final Object data;
    
    public FailoverData(DataType dataType, Object data) {
        this.data = data;
        this.dataType = dataType;
    }
    
    public enum DataType {
        /**
         * 注册中心
         */
        naming,
        /**
         * 配置中心
         */
        config
    }
    
    public DataType getDataType() {
        return dataType;
    }
    
    public Object getData() {
        return data;
    }
}
