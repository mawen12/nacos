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

package com.alibaba.nacos.naming.core.v2.service.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.ClientOperationService;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 为临时客户端和服务提供操作服务
 *
 * @author xiweng.yy
 */
@Component("ephemeralClientOperationService")
public class EphemeralClientOperationServiceImpl implements ClientOperationService {
    
    private final ClientManager clientManager;
    
    public EphemeralClientOperationServiceImpl(ClientManagerDelegate clientManager) {
        this.clientManager = clientManager;
    }
    
    @Override
    public void registerInstance(Service service, Instance instance, String clientId) throws NacosException {
        /**
         * 校验实例非空，超时时间，集群名称
         */
        NamingUtils.checkInstanceIsLegal(instance);
        /**
         * 从ServiceManger中获取对应服务，如果服务不存在，则加入到{@link ServiceManager#singletonRepository}, {@link ServiceManager#namespaceSingletonMaps}，
         * 并发布{@link com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.ServiceMetadataEvent}，
         * 如果服务已经存在，代表该服务下已经存在了实例，可以组成集群
         */
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        /**
         * 服务必须是临时的
         */
        if (!singleton.isEphemeral()) {
            throw new NacosRuntimeException(NacosException.INVALID_PARAM, String.format("Current service %s is persistent service, can't register ephemeral instance.", singleton.getGroupedServiceName()));
        }

        /**
         * 从ClientManager中获取对应客户端，如果客户端不存在，则返回null
         */
        Client client = clientManager.getClient(clientId);
        /**
         * 校验客户端非空，且为ephemeral
         * 如果客户端为空，则代表执行实例注册的客户端已经断开链接，那就无法执行注册了，直接报错
         */
        checkClientIsLegal(client, clientId);
        /**
         * 从 Instance -> InstancePublishInfo
         */
        InstancePublishInfo instanceInfo = getPublishInfo(instance);
        /**
         * 将服务信息和实例发布信息写入到客户端的{@link com.alibaba.nacos.naming.core.v2.client.impl.ConnectionBasedClient#publishers}
         */
        client.addServiceInstance(singleton, instanceInfo);
        /**
         * 更新客户端的最后更新时间
         */
        client.setLastUpdatedTime();
        /**
         * 更新客户端的编辑次数
         */
        client.recalculateRevision();
        /**
         * 发布客户端注册服务事件，将服务和客户端写入到{@link com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager#publisherIndexes}中，
         * 并通知到所有监听的客户端
         */
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
        /**
         * 发布实例元数据事件
         */
        NotifyCenter.publishEvent(new MetadataEvent.InstanceMetadataEvent(singleton, instanceInfo.getMetadataId(), false));
    }
    
    @Override
    public void batchRegisterInstance(Service service, List<Instance> instances, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        if (!singleton.isEphemeral()) {
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current service %s is persistent service, can't batch register ephemeral instance.",
                            singleton.getGroupedServiceName()));
        }
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        BatchInstancePublishInfo batchInstancePublishInfo = new BatchInstancePublishInfo();
        List<InstancePublishInfo> resultList = new ArrayList<>();
        for (Instance instance : instances) {
            InstancePublishInfo instanceInfo = getPublishInfo(instance);
            resultList.add(instanceInfo);
        }
        batchInstancePublishInfo.setInstancePublishInfos(resultList);
        client.addServiceInstance(singleton, batchInstancePublishInfo);
        client.setLastUpdatedTime();
        client.recalculateRevision();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId));
        NotifyCenter.publishEvent(
                new MetadataEvent.InstanceMetadataEvent(singleton, batchInstancePublishInfo.getMetadataId(), false));
    }
    
    @Override
    public void deregisterInstance(Service service, Instance instance, String clientId) {
        /**
         * 如果服务端不存在该服务，代表之前服务已经注销，或者没有任何实例注册过该服务，则直接返回
         */
        if (!ServiceManager.getInstance().containSingleton(service)) {
            Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", service);
            return;
        }
        /**
         * 获取服务信息，如果服务信息不存在，应该之前判断，无法走到这一步
         */
        Service singleton = ServiceManager.getInstance().getSingleton(service);
        /**
         * 获取对应的客户端
         */
        Client client = clientManager.getClient(clientId);
        /**
         * 校验客户端非空，并且客户端必须是临时的
         */
        checkClientIsLegal(client, clientId);
        InstancePublishInfo removedInstance = client.removeServiceInstance(singleton);
        client.setLastUpdatedTime();
        client.recalculateRevision();
        if (null != removedInstance) {
            NotifyCenter.publishEvent(new ClientOperationEvent.ClientDeregisterServiceEvent(singleton, clientId));
            NotifyCenter.publishEvent(
                    new MetadataEvent.InstanceMetadataEvent(singleton, removedInstance.getMetadataId(), true));
        }
    }
    
    @Override
    public void subscribeService(Service service, Subscriber subscriber, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        client.addServiceSubscriber(singleton, subscriber);
        client.setLastUpdatedTime();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientSubscribeServiceEvent(singleton, clientId));
    }
    
    @Override
    public void unsubscribeService(Service service, Subscriber subscriber, String clientId) {
        Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
        Client client = clientManager.getClient(clientId);
        checkClientIsLegal(client, clientId);
        client.removeServiceSubscriber(singleton);
        client.setLastUpdatedTime();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientUnsubscribeServiceEvent(singleton, clientId));
    }

    private void checkClientIsLegal(Client client, String clientId) {
        /**
         * 客户端非空校验
         */
        if (client == null) {
            Loggers.SRV_LOG.warn("Client connection {} already disconnect", clientId);
            throw new NacosRuntimeException(NacosException.CLIENT_DISCONNECT,
                    String.format("Client [%s] connection already disconnect, can't register ephemeral instance.",
                            clientId));
        }
        /**
         * 客户端必须是ephemeral校验
         */
        if (!client.isEphemeral()) {
            Loggers.SRV_LOG.warn("Client connection {} type is not ephemeral", clientId);
            throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                    String.format("Current client [%s] is persistent client, can't register ephemeral instance.",
                            clientId));
        }
    }
}
