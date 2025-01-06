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

package com.alibaba.nacos.api.naming;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.selector.NamingSelector;
import com.alibaba.nacos.api.selector.AbstractSelector;

import java.util.List;

/**
 * 注册中心服务
 *
 * @author nkorange
 */
public interface NamingService {
    
    /**
     * 将一个实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param ip          实例ip
     * @param port        实例端口
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, String ip, int port) throws NacosException;
    
    /**
     * 将一个实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param ip          实例ip
     * @param port        实例端口
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException;
    
    /**
     * 将一个带有特定集群名称的实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param ip          实例ip
     * @param port        实例端口
     * @param clusterName 实例集群名称
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException;
    
    /**
     * 将一个带有特定集群名称的实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param ip          实例ip
     * @param port        实例端口
     * @param clusterName 实例集群名称
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException;
    
    /**
     * 将一个带有特定属性的实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param instance    服务实例
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, Instance instance) throws NacosException;
    
    /**
     * 将一个带有特定属性的实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param instance    服务实例
     * @throws NacosException nacos exception
     */
    void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException;
    
    /**
     * 将一批带有特定属性的实例注册到Nacos
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param instances   服务实例列表
     * @throws NacosException nacos exception
     * @since 2.1.1
     */
    void batchRegisterInstance(String serviceName, String groupName, List<Instance> instances) throws NacosException;
    
    /**
     * 从Nacos中一批特定实例注销
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param instances   服务实例列表
     * @throws NacosException nacos exception
     * @since 2.2.0
     */
    void batchDeregisterInstance(String serviceName, String groupName, List<Instance> instances) throws NacosException;
    
    /**
     * 从Nacos中注销实例
     *
     * @param serviceName 服务名称
     * @param ip          实例ip
     * @param port        实例端口
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, String ip, int port) throws NacosException;
    
    /**
     * 从Nacos中注销实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param ip          实例ip
     * @param port        实例端口
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException;
    
    /**
     * 从Nacos中注销一批指定集群的实例
     *
     * @param serviceName 服务名称
     * @param ip          实例ip
     * @param port        实例端口
     * @param clusterName 集群名称
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException;
    
    /**
     * 从Nacos中注销指定集群的实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param ip          实例ip
     * @param port        实例端口
     * @param clusterName 集群名称
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException;
    
    /**
     * 从Nacos中注销实例
     *
     * @param serviceName 服务名称
     * @param instance    实例信息
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, Instance instance) throws NacosException;
    
    /**
     * 从Nacos中注销实例
     *
     * @param serviceName 服务名称
     * @param groupName   分组名称
     * @param instance    实例信息
     * @throws NacosException nacos exception
     */
    void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException;
    
    /**
     * 获取指定服务的所有实例
     *
     * @param serviceName 服务名称
     * @return A list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName) throws NacosException;
    
    /**
     * 获取指定服务的所有实例
     *
     * @param serviceName 服务名称
     * @param groupName   分组名称
     * @return A list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException;
    
    /**
     * 获取指定服务的所有实例
     *
     * @param serviceName 服务名称
     * @param subscribe   是否订阅该服务
     * @return A list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException;
    
    /**
     * 获取指定服务的所有实例
     *
     * @param serviceName 服务名称
     * @param groupName   分组名称
     * @param subscribe   是否订阅该服务
     * @return A list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe) throws NacosException;
    
    /**
     * 获取指定服务指定集群的所有实例
     *
     * @param serviceName 服务名称
     * @param clusters    集群名称列表
     * @return A list of qualified instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException;
    
    /**
     * 获取指定服务指定集群的所有实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param clusters    集群名称列表
     * @return A list of qualified instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters) throws NacosException;
    
    /**
     * 获取指定服务指定集群的所有实例
     *
     * @param serviceName 服务名称
     * @param clusters    集群名称列表
     * @param subscribe   是否订阅该服务
     * @return A list of qualified instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe) throws NacosException;
    
    /**
     * 获取指定服务指定集群的所有实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param clusters    集群名称列表
     * @param subscribe   是否订阅该服务
     * @return A list of qualified instance
     * @throws NacosException nacos exception
     */
    List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters, boolean subscribe)
            throws NacosException;
    
    /**
     * 获取特定服务的实例
     *
     * @param serviceName 服务名称
     * @param healthy     服务状态是否健康
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException;
    
    /**
     * 获取特定服务的实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param healthy     服务状态是否健康
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException;
    
    /**
     * 获取特定服务的实例
     *
     * @param serviceName 服务名称
     * @param healthy     服务状态是否健康
     * @param subscribe   是否订阅该服务
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe) throws NacosException;
    
    /**
     * 获取特定服务的实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param healthy     服务状态是否健康
     * @param subscribe   是否订阅该服务
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe)
            throws NacosException;
    
    /**
     * 获取特定服务特定集群的实例
     *
     * @param serviceName 服务名称
     * @param clusters    集群名称列表
     * @param healthy     服务状态是否健康
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy) throws NacosException;
    
    /**
     * 获取特定服务特定集群的实例
     *
     * @param serviceName 服务名称
     * @param groupName   服务所在分组
     * @param clusters    集群名称列表
     * @param healthy     服务状态是否健康
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy)
            throws NacosException;
    
    /**
     * Get qualified instances within specified clusters of service.
     *
     * @param serviceName name of service
     * @param clusters    list of cluster
     * @param healthy     a flag to indicate returning healthy or unhealthy instances
     * @param subscribe   if subscribe the service
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy, boolean subscribe)
            throws NacosException;
    
    /**
     * Get qualified instances within specified clusters of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    list of cluster
     * @param healthy     a flag to indicate returning healthy or unhealthy instances
     * @param subscribe   if subscribe the service
     * @return A qualified list of instance
     * @throws NacosException nacos exception
     */
    List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy,
            boolean subscribe) throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName) throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException;
    
    /**
     * select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param subscribe   if subscribe the service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException;
    
    /**
     * select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param subscribe   if subscribe the service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe) throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param clusters    a list of clusters should the instance belongs to
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    a list of clusters should the instance belongs to
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters)
            throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param clusters    a list of clusters should the instance belongs to
     * @param subscribe   if subscribe the service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException;
    
    /**
     * Select one healthy instance of service using predefined load balance strategy.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    a list of clusters should the instance belongs to
     * @param subscribe   if subscribe the service
     * @return qualified instance
     * @throws NacosException nacos exception
     */
    Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters, boolean subscribe)
            throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, EventListener listener) throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param selector    selector of instances
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, NamingSelector selector, EventListener listener) throws NacosException;
    
    /**
     * Subscribe service to receive events of instances alteration.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param selector    selector of instances
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void subscribe(String serviceName, String groupName, NamingSelector selector, EventListener listener)
            throws NacosException;
    
    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, EventListener listener) throws NacosException;
    
    /**
     * unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException;
    
    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException;
    
    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param clusters    list of cluster
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException;
    
    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param selector    selector of instances
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, NamingSelector selector, EventListener listener) throws NacosException;
    
    /**
     * Unsubscribe event listener of service.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param selector    selector of instances
     * @param listener    event listener
     * @throws NacosException nacos exception
     */
    void unsubscribe(String serviceName, String groupName, NamingSelector selector, EventListener listener)
            throws NacosException;
    
    /**
     * Get all service names from server.
     *
     * @param pageNo   page index
     * @param pageSize page size
     * @return list of service names
     * @throws NacosException nacos exception
     */
    ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException;
    
    /**
     * Get all service names from server.
     *
     * @param pageNo    page index
     * @param pageSize  page size
     * @param groupName group name
     * @return list of service names
     * @throws NacosException nacos exception
     */
    ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException;
    
    /**
     * Get all service names from server with selector.
     *
     * @param pageNo   page index
     * @param pageSize page size
     * @param selector selector to filter the resource
     * @return list of service names
     * @throws NacosException nacos exception
     * @since 0.7.0
     */
    ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector) throws NacosException;
    
    /**
     * Get all service names from server with selector.
     *
     * @param pageNo    page index
     * @param pageSize  page size
     * @param groupName group name
     * @param selector  selector to filter the resource
     * @return list of service names
     * @throws NacosException nacos exception
     */
    ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException;
    
    /**
     * Get all subscribed services of current client.
     *
     * @return subscribed services
     * @throws NacosException nacos exception
     */
    List<ServiceInfo> getSubscribeServices() throws NacosException;
    
    /**
     * 返回Nacos状态
     *
     * @return is server healthy
     */
    String getServerStatus();
    
    /**
     * 中止注册中心服务
     *
     * @throws NacosException exception.
     */
    void shutDown() throws NacosException;
}
