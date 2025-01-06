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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Cluster;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.naming.constants.FieldsConstants;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.ClusterMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.pojo.ClusterInfo;
import com.alibaba.nacos.naming.pojo.IpAddressInfo;
import com.alibaba.nacos.naming.pojo.ServiceDetailInfo;
import com.alibaba.nacos.naming.pojo.ServiceView;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Catalog service for v2.x .
 *
 * @author xiweng.yy
 */
@Component()
public class CatalogServiceV2Impl implements CatalogService {
    
    private final ServiceStorage serviceStorage;
    
    private final NamingMetadataManager metadataManager;
    
    private static final int DEFAULT_PORT = 80;
    
    public CatalogServiceV2Impl(ServiceStorage serviceStorage, NamingMetadataManager metadataManager) {
        this.serviceStorage = serviceStorage;
        this.metadataManager = metadataManager;
    }

    /**
     * 返回指定命名空间下服务的服务信息、服务元信息、服务下的集群信息
     *
     * @param namespaceId namespace id of service
     * @param groupName   group name of service
     * @param serviceName service name
     * @return
     * @throws NacosException
     */
    @Override
    public Object getServiceDetail(String namespaceId, String groupName, String serviceName) throws NacosException {
        /**
         * 构建服务
         */
        Service service = Service.newService(namespaceId, groupName, serviceName);
        /**
         * 如果服务管理器中不存在对应服务，则代表服务不存在
         */
        if (!ServiceManager.getInstance().containSingleton(service)) {
            throw new NacosException(NacosException.NOT_FOUND, String.format("service %s@@%s is not found!", groupName, serviceName));
        }
        /**
         * 获取服务的元数据，如果不存在，就创建一个
         */
        Optional<ServiceMetadata> metadata = metadataManager.getServiceMetadata(service);
        ServiceMetadata detailedService = metadata.orElseGet(ServiceMetadata::new);

        ObjectNode serviceObject = JacksonUtils.createEmptyJsonNode();
        // 服务
        serviceObject.put(FieldsConstants.NAME, serviceName);
        // 分组
        serviceObject.put(FieldsConstants.GROUP_NAME, groupName);
        // 服务保护阈值
        serviceObject.put(FieldsConstants.PROTECT_THRESHOLD, detailedService.getProtectThreshold());
        // 服务选择器
        serviceObject.replace(FieldsConstants.SELECTOR, JacksonUtils.transferToJsonNode(detailedService.getSelector()));
        // 服务元信息
        serviceObject.replace(FieldsConstants.METADATA, JacksonUtils.transferToJsonNode(detailedService.getExtendData()));
        
        ObjectNode detailView = JacksonUtils.createEmptyJsonNode();
        // 服务对象
        detailView.replace(FieldsConstants.SERVICE, serviceObject);
        
        List<com.alibaba.nacos.api.naming.pojo.Cluster> clusters = new ArrayList<>();

        /**
         * 获取服务下的所有集群名称
         */
        for (String each : serviceStorage.getClusters(service)) {
            /**
             * 获取对应的元数据信息
             */
            ClusterMetadata clusterMetadata = detailedService.getClusters().containsKey(each) ? detailedService.getClusters().get(each) : new ClusterMetadata();
            com.alibaba.nacos.api.naming.pojo.Cluster clusterView = new Cluster();
            clusterView.setName(each);
            clusterView.setHealthChecker(clusterMetadata.getHealthChecker());
            clusterView.setMetadata(clusterMetadata.getExtendData());
            clusterView.setUseIPPort4Check(clusterMetadata.isUseInstancePortForCheck());
            clusterView.setDefaultPort(DEFAULT_PORT);
            clusterView.setDefaultCheckPort(clusterMetadata.getHealthyCheckPort());
            clusterView.setServiceName(service.getGroupedServiceName());
            clusters.add(clusterView);
        }

        /**
         * 服务集群信息
         */
        detailView.replace(FieldsConstants.CLUSTERS, JacksonUtils.transferToJsonNode(clusters));
        
        return detailView;
    }
    
    @Override
    public List<? extends Instance> listInstances(String namespaceId, String groupName, String serviceName, String clusterName) throws NacosException {
        /**
         * 构建服务
         */
        Service service = Service.newService(namespaceId, groupName, serviceName);
        /**
         * 如果服务管理器中不存在对应服务，则代表服务不存在
         */
        if (!ServiceManager.getInstance().containSingleton(service)) {
            throw new NacosException(NacosException.NOT_FOUND, String.format("service %s@@%s is not found!", groupName, serviceName));
        }
        /**
         * 如果服务下不存在对应的集群名称，则代表指定集群不存在
         */
        if (!serviceStorage.getClusters(service).contains(clusterName)) {
            throw new NacosException(NacosException.NOT_FOUND, "cluster " + clusterName + " is not found!");
        }
        /**
         * 获取服务的服务信息，其中包含实例列表
         */
        ServiceInfo serviceInfo = serviceStorage.getData(service);
        /**
         * 从{@link ServiceInfo#hosts}中过滤匹配指定集群的实例
         */
        ServiceInfo result = ServiceUtil.selectInstances(serviceInfo, clusterName);
        return result.getHosts();
    }
    
    @Override
    public List<? extends Instance> listAllInstances(String namespaceId, String groupName, String serviceName) {
        Service service = Service.newService(namespaceId, groupName, serviceName);
        if (!ServiceManager.getInstance().containSingleton(service)) {
            return Collections.EMPTY_LIST;
        }
        
        ServiceInfo serviceInfo = serviceStorage.getData(service);
        
        return serviceInfo.getHosts();
    }
    
    @Override
    public Object pageListService(String namespaceId, String groupName, String serviceName, int pageNo, int pageSize,
            String instancePattern, boolean ignoreEmptyService) throws NacosException {
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        List<ServiceView> serviceViews = new LinkedList<>();
        /**
         * 从{@link ServiceManager#namespaceSingletonMaps}获取匹配特定命名空间、分组名称和服务名称
         */
        Collection<Service> services = patternServices(namespaceId, groupName, serviceName);
        /**
         * 是否忽略空服务（即其下没有任何的实例）
         */
        if (ignoreEmptyService) {
            services = services.stream().filter(each -> 0 != serviceStorage.getData(each).ipCount()).collect(Collectors.toList());
        }
        result.put(FieldsConstants.COUNT, services.size());
        services = doPage(services, pageNo - 1, pageSize);
        for (Service each : services) {
            ServiceMetadata serviceMetadata = metadataManager.getServiceMetadata(each).orElseGet(ServiceMetadata::new);
            ServiceView serviceView = new ServiceView();
            serviceView.setName(each.getName());
            serviceView.setGroupName(each.getGroup());
            serviceView.setClusterCount(serviceStorage.getClusters(each).size());
            serviceView.setIpCount(serviceStorage.getData(each).ipCount());
            serviceView.setHealthyInstanceCount(countHealthyInstance(serviceStorage.getData(each)));
            serviceView.setTriggerFlag(isProtectThreshold(serviceView, serviceMetadata) ? "true" : "false");
            serviceViews.add(serviceView);
        }
        result.set(FieldsConstants.SERVICE_LIST, JacksonUtils.transferToJsonNode(serviceViews));
        return result;
    }
    
    private int countHealthyInstance(ServiceInfo data) {
        int result = 0;
        for (Instance each : data.getHosts()) {
            if (each.isHealthy()) {
                result++;
            }
        }
        return result;
    }
    
    private boolean isProtectThreshold(ServiceView serviceView, ServiceMetadata metadata) {
        return (serviceView.getHealthyInstanceCount() * 1.0 / serviceView.getIpCount()) <= metadata
                .getProtectThreshold();
    }
    
    @Override
    public Object pageListServiceDetail(String namespaceId, String groupName, String serviceName, int pageNo,
            int pageSize) throws NacosException {
        List<ServiceDetailInfo> result = new ArrayList<>();
        Collection<Service> services = patternServices(namespaceId, groupName, serviceName);
        services = doPage(services, pageNo - 1, pageSize);
        for (Service each : services) {
            ServiceDetailInfo serviceDetailInfo = new ServiceDetailInfo();
            serviceDetailInfo.setServiceName(each.getName());
            serviceDetailInfo.setGroupName(each.getGroup());
            ServiceMetadata serviceMetadata = metadataManager.getServiceMetadata(each).orElseGet(ServiceMetadata::new);
            serviceDetailInfo.setMetadata(serviceMetadata.getExtendData());
            serviceDetailInfo.setClusterMap(getClusterMap(each));
            result.add(serviceDetailInfo);
        }
        return result;
    }
    
    private Map<String, ClusterInfo> getClusterMap(Service service) {
        Map<String, ClusterInfo> result = new HashMap<>(1);
        for (Instance each : serviceStorage.getData(service).getHosts()) {
            final IpAddressInfo info = transferToIpAddressInfo(each);
            if (!result.containsKey(each.getClusterName())) {
                ClusterInfo clusterInfo = new ClusterInfo();
                clusterInfo.setHosts(new LinkedList<>());
                result.put(each.getClusterName(), clusterInfo);
            }
            result.get(each.getClusterName()).getHosts().add(info);
        }
        return result;
    }
    
    private IpAddressInfo transferToIpAddressInfo(Instance instance) {
        IpAddressInfo result = new IpAddressInfo();
        result.setIp(instance.getIp());
        result.setPort(instance.getPort());
        result.setEnabled(instance.isEnabled());
        result.setValid(instance.isHealthy());
        result.setWeight(instance.getWeight());
        result.setMetadata(instance.getMetadata());
        return result;
    }
    
    private Collection<Service> patternServices(String namespaceId, String group, String serviceName) {
        /**
         * 如果服务名称和分组名称均为空，代表不进行过滤，直接查询该租户下所有的服务
         */
        boolean noFilter = StringUtils.isBlank(serviceName) && StringUtils.isBlank(group);
        if (noFilter) {
            return ServiceManager.getInstance().getSingletons(namespaceId);
        }
        Collection<Service> result = new LinkedList<>();
        /**
         * 分组+服务名称 -> 正则表达式
         */
        StringJoiner regex = new StringJoiner(Constants.SERVICE_INFO_SPLITER);
        regex.add(getRegexString(group));
        regex.add(getRegexString(serviceName));
        String regexString = regex.toString();

        for (Service each : ServiceManager.getInstance().getSingletons(namespaceId)) {
            /**
             * 如果格式匹配，则加入到结果中
             */
            if (each.getGroupedServiceName().matches(regexString)) {
                result.add(each);
            }
        }
        return result;
    }
    
    private String getRegexString(String target) {
        return StringUtils.isBlank(target) ? Constants.ANY_PATTERN
                : Constants.ANY_PATTERN + target + Constants.ANY_PATTERN;
    }
    
    private Collection<Service> doPage(Collection<Service> services, int pageNo, int pageSize) {
        if (services.size() < pageSize) {
            return services;
        }
        Collection<Service> result = new LinkedList<>();
        int i = 0;
        for (Service each : services) {
            if (i++ < pageNo * pageSize) {
                continue;
            }
            result.add(each);
            if (result.size() >= pageSize) {
                break;
            }
        }
        return result;
    }
}
