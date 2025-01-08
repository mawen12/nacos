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

import java.util.Map;

/**
 * 故障转移服务接口
 *
 * @author Nacos
 */
public interface FailoverDataSource {
    
    /**
     * 获取当前灾难恢复交换机，可以设置或读取当前是否开启了灾难恢复
     *
     * @return
     */
    FailoverSwitch getSwitch();
    
    
    /**
     * 获取当前灾难恢复数据
     *
     * @return map
     */
    Map<String, FailoverData> getFailoverData();
    
}
