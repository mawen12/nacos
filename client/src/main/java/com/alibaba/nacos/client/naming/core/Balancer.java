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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.utils.Chooser;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 实例的负载均衡器，用于服务订阅和发起请求时，对于存在多个实例的服务，提供不同的选择算法：
 * <ul>
 *     <li>基于权重的随机选择</li>
 * </ul>
 *
 * @author xuanyin
 */
public class Balancer {

    /**
     * 基于权重的随机选择
     */
    public static class RandomByWeight {
    
        /**
         * Select all instance.
         *
         * @param serviceInfo service information
         * @return all instance of services
         */
        public static List<Instance> selectAll(ServiceInfo serviceInfo) {
            /**
             * 获取该服务下所有的实例，包含健康和不健康的
             */
            List<Instance> hosts = serviceInfo.getHosts();
            if (CollectionUtils.isEmpty(hosts)) {
                throw new IllegalStateException("no host to srv for serviceInfo: " + serviceInfo.getName());
            }
            return hosts;
        }
    
        /**
         * Random select one instance from service.
         *
         * @param dom service
         * @return random instance
         */
        public static Instance selectHost(ServiceInfo dom) {
            /**
             * 基于权重随机选择一个实例
             */
            return getHostByRandomWeight(selectAll(dom));
        }
    }
    
    /**
     * 基于权重随机选择一个实例，其中仅会在权重>=0的实例中选择
     *
     * @param hosts The list of the host.
     * @return The random-weight result of the host
     */
    protected static Instance getHostByRandomWeight(List<Instance> hosts) {
        NAMING_LOGGER.debug("entry randomWithWeight");
        /**
         * 如果实例不存在，代表无法选择，则直接返回
         */
        if (hosts == null || hosts.size() == 0) {
            NAMING_LOGGER.debug("hosts == null || hosts.size() == 0");
            return null;
        }
        NAMING_LOGGER.debug("new Chooser");
        /**
         * 过滤出所有健康状态的实例，并保存到集合中。
         * 此处应强制采用ArrayList，因为底层的默认轮询器采用基于索引获取元素的方式
         */
        List<Pair<Instance>> hostsWithWeight = new ArrayList<>();
        for (Instance host : hosts) {
            if (host.isHealthy()) {
                hostsWithWeight.add(new Pair<Instance>(host, host.getWeight()));
            }
        }
        NAMING_LOGGER.debug("for (Host host : hosts)");
        /**
         * 构造选择器
         */
        Chooser<String, Instance> vipChooser = new Chooser<>("www.taobao.com");
        vipChooser.refresh(hostsWithWeight);
        NAMING_LOGGER.debug("vipChooser.refresh");
        /**
         * 返回基于权重的随机的实例
         */
        return vipChooser.randomWithWeight();
    }
}
