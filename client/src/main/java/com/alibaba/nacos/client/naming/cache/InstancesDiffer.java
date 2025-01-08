/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.event.InstancesDiff;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 服务信息差异对比工具
 *
 * @author xiweng.yy
 */
public final class InstancesDiffer {
    
    /**
     * 返回旧服务信息和新服务信息的差异对比结果
     *
     * @param oldService old service info
     * @param newService new service info
     * @return {@link InstancesDiff} of the differences between old and new service info.
     */
    public InstancesDiff doDiff(ServiceInfo oldService, ServiceInfo newService) {
        InstancesDiff instancesDiff = new InstancesDiff();
        if (null == oldService) {
            /**
             * 旧服务为空，则代表服务是新增的
             */
            NAMING_LOGGER.info("init new ips({}) service: {} -> {}", newService.ipCount(), newService.getKey(), JacksonUtils.toJson(newService.getHosts()));
            instancesDiff.setAddedInstances(newService.getHosts());
            return instancesDiff;
        }
        if (oldService.getLastRefTime() > newService.getLastRefTime()) {
            /**
             * 旧服务比新服务的更新时间还要新，就代表数据存在问题，直接返回
             */
            NAMING_LOGGER.warn("out of date data received, old-t: {}, new-t: {}", oldService.getLastRefTime(), newService.getLastRefTime());
            return instancesDiff;
        }

        /**
         * 转换为Map进行比对
         *
         * TODO by mawen 也许应该设置1.0f
         */
        Map<String, Instance> oldHostMap = new HashMap<>(oldService.getHosts().size());
        for (Instance host : oldService.getHosts()) {
            oldHostMap.put(host.toInetAddr(), host);
        }
        Map<String, Instance> newHostMap = new HashMap<>(newService.getHosts().size());
        for (Instance host : newService.getHosts()) {
            newHostMap.put(host.toInetAddr(), host);
        }
        
        Set<Instance> modHosts = new HashSet<>();
        Set<Instance> newHosts = new HashSet<>();
        Set<Instance> remvHosts = new HashSet<>();
        
        List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<>(newHostMap.entrySet());
        for (Map.Entry<String, Instance> entry : newServiceHosts) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(), oldHostMap.get(key).toString())) {
                modHosts.add(host);
                continue;
            }
            
            if (!oldHostMap.containsKey(key)) {
                newHosts.add(host);
            }
        }
        
        for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (newHostMap.containsKey(key)) {
                continue;
            }
            
            //add to remove hosts
            remvHosts.add(host);
        }
        
        if (newHosts.size() > 0) {
            NAMING_LOGGER.info("new ips({}) service: {} -> {}", newHosts.size(), newService.getKey(), JacksonUtils.toJson(newHosts));
            instancesDiff.setAddedInstances(newHosts);
        }
        
        if (remvHosts.size() > 0) {
            NAMING_LOGGER.info("removed ips({}) service: {} -> {}", remvHosts.size(), newService.getKey(), JacksonUtils.toJson(remvHosts));
            instancesDiff.setRemovedInstances(remvHosts);
        }
        
        if (modHosts.size() > 0) {
            NAMING_LOGGER.info("modified ips({}) service: {} -> {}", modHosts.size(), newService.getKey(), JacksonUtils.toJson(modHosts));
            instancesDiff.setModifiedInstances(modHosts);
        }
        return instancesDiff;
    }
}
