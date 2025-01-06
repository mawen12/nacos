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

package com.alibaba.nacos.client.naming.remote.gprc.redo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.BatchInstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.InstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.RedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.SubscriberRedoData;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.task.AbstractExecuteTask;

/**
 * 恢复任务，定时检查{@link NamingGrpcRedoService#registeredInstances}，
 * 其中由
 *
 * @author xiweng.yy
 */
public class RedoScheduledTask extends AbstractExecuteTask {

    /**
     * 用于执行rpc请求的客户端代理
     */
    private final NamingGrpcClientProxy clientProxy;

    /**
     * 用于获取实例恢复数据，用于构造执行rpc请求的{@link com.alibaba.nacos.api.naming.remote.request.InstanceRequest},
     * 并且该任务中也会更新实例恢复数据
     */
    private final NamingGrpcRedoService redoService;
    
    public RedoScheduledTask(NamingGrpcClientProxy clientProxy, NamingGrpcRedoService redoService) {
        this.clientProxy = clientProxy;
        this.redoService = redoService;
    }
    
    @Override
    public void run() {
        /**
         * 如果恢复服务断开链接，代表无法与Nacos Server通信，便跳过该任务
         */
        if (!redoService.isConnected()) {
            LogUtils.NAMING_LOGGER.warn("Grpc Connection is disconnect, skip current redo task");
            return;
        }
        try {
            /**
             * 获取{@link InstanceRedoData#isNeedRedo()}为true的数据，即获取需要执行恢复操作的实例，
             * 并根据不同的恢复类型执行不同的操作
             * <ul>
             *     <li>{@link com.alibaba.nacos.client.naming.remote.gprc.redo.data.RedoData.RedoType.REGISTER}，便发送rpc请求到Nacos Server，执行实例注册</li>
             *     <li>{@link com.alibaba.nacos.client.naming.remote.gprc.redo.data.RedoData.RedoType.UNREGISTER}, 便发送rpc请求到Nacos Server，执行实例注销</li>
             *     <li>{@link com.alibaba.nacos.client.naming.remote.gprc.redo.data.RedoData.RedoType.REMOVE}，将该实例从客户端中移除，因为该实例无需再次使用</li>
             * </ul>
             */
            redoForInstances();
            /**
             *
             */
            redoForSubscribes();
        } catch (Exception e) {
            LogUtils.NAMING_LOGGER.warn("Redo task run with unexpected exception: ", e);
        }
    }
    
    private void redoForInstances() {
        /**
         * 获取需要恢复的实例，并取出依次执行恢复操作
         */
        for (InstanceRedoData each : redoService.findInstanceRedoData()) {
            try {
                redoForInstance(each);
            } catch (NacosException e) {
                LogUtils.NAMING_LOGGER.error("Redo instance operation {} for {}@@{} failed. ", each.getRedoType(), each.getGroupName(), each.getServiceName(), e);
            }
        }
    }
    
    private void redoForInstance(InstanceRedoData redoData) throws NacosException {
        /**
         * 获取该实例的恢复类型
         */
        RedoData.RedoType redoType = redoData.getRedoType();
        /**
         * 获取实例的服务名称
         */
        String serviceName = redoData.getServiceName();
        /**
         * 获取分组名称
         */
        String groupName = redoData.getGroupName();
        LogUtils.NAMING_LOGGER.info("Redo instance operation {} for {}@@{}", redoType, groupName, serviceName);
        switch (redoType) {
            case REGISTER:
                // 如果客户端已经关闭服务，则跳过该任务
                if (isClientDisabled()) {
                    return;
                }
                // 发送rpc请求到Nacos Server，并更新实例的注册状态
                processRegisterRedoType(redoData, serviceName, groupName);
                break;

            case UNREGISTER:
                // 如果客户端已经关闭服务，则跳过该任务
                if (isClientDisabled()) {
                    return;
                }
                // 发送rpc请求到Nacos Server，并更新实例的注销状态
                clientProxy.doDeregisterService(serviceName, groupName, redoData.get());
                break;
            case REMOVE:
                // 将内存中的实例移除
                redoService.removeInstanceForRedo(serviceName, groupName);
                break;
            default:
        }
    }
    
    private void processRegisterRedoType(InstanceRedoData redoData, String serviceName, String groupName) throws NacosException {
        if (redoData instanceof BatchInstanceRedoData) {
            // Execute Batch Register
            BatchInstanceRedoData batchInstanceRedoData = (BatchInstanceRedoData) redoData;
            clientProxy.doBatchRegisterService(serviceName, groupName, batchInstanceRedoData.getInstances());
            return;
        }
        /**
         *
         */
        clientProxy.doRegisterService(serviceName, groupName, redoData.get());
    }
    
    private void redoForSubscribes() {
        for (SubscriberRedoData each : redoService.findSubscriberRedoData()) {
            try {
                redoForSubscribe(each);
            } catch (NacosException e) {
                LogUtils.NAMING_LOGGER.error("Redo subscriber operation {} for {}@@{}#{} failed. ", each.getRedoType(),
                        each.getGroupName(), each.getServiceName(), each.get(), e);
            }
        }
    }
    
    private void redoForSubscribe(SubscriberRedoData redoData) throws NacosException {
        RedoData.RedoType redoType = redoData.getRedoType();
        String serviceName = redoData.getServiceName();
        String groupName = redoData.getGroupName();
        String cluster = redoData.get();
        LogUtils.NAMING_LOGGER.info("Redo subscriber operation {} for {}@@{}#{}", redoType, groupName, serviceName, cluster);
        switch (redoData.getRedoType()) {
            case REGISTER:
                if (isClientDisabled()) {
                    return;
                }
                clientProxy.doSubscribe(serviceName, groupName, cluster);
                break;
            case UNREGISTER:
                if (isClientDisabled()) {
                    return;
                }
                clientProxy.doUnsubscribe(serviceName, groupName, cluster);
                break;
            case REMOVE:
                redoService.removeSubscriberForRedo(redoData.getServiceName(), redoData.getGroupName(), redoData.get());
                break;
            default:
        }
    }
    
    private boolean isClientDisabled() {
        return !clientProxy.isEnable();
    }
}
