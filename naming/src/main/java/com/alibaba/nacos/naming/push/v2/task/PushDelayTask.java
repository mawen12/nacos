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

package com.alibaba.nacos.naming.push.v2.task;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;

import java.util.HashSet;
import java.util.Set;

/**
 * Nacos注册中心推送延迟任务，以下场景触发：
 * <ul>
 *     <li>客户端注册服务时，通过{@link com.alibaba.nacos.naming.push.v2.NamingSubscriberServiceV2Impl#onEvent(Event)}创建任务</li>
 *     <li>该任务的处理器为：{@link PushDelayTaskExecuteEngine.PushDelayTaskProcessor}</li>
 *     <li>该任务会被处理器转换为{@link PushExecuteTask}</li>
 * </ul>
 *
 * @author xiweng.yy
 */
public class PushDelayTask extends AbstractDelayTask {
    
    private final Service service;
    
    private boolean pushToAll;
    
    private Set<String> targetClients;
    
    public PushDelayTask(Service service, long delay) {
        this.service = service;
        pushToAll = true;
        targetClients = null;
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }
    
    public PushDelayTask(Service service, long delay, String targetClient) {
        this.service = service;
        this.pushToAll = false;
        this.targetClients = new HashSet<>(1);
        this.targetClients.add(targetClient);
        setTaskInterval(delay);
        setLastProcessTime(System.currentTimeMillis());
    }
    
    @Override
    public void merge(AbstractDelayTask task) {
        /**
         * 仅处理相同类型的任务
         */
        if (!(task instanceof PushDelayTask)) {
            return;
        }
        PushDelayTask oldTask = (PushDelayTask) task;
        /**
         * 如果本次任务需要通知到全部客户端，或者之前需要通知到全部客户端，则取最大；否则将两次推送的客户端进行合并
         */
        if (isPushToAll() || oldTask.isPushToAll()) {
            pushToAll = true;
            targetClients = null;
        } else {
            targetClients.addAll(oldTask.getTargetClients());
        }
        /**
         * 取最老的执行时间，这是因为存在旧任务，该任务的执行时间需要提前
         */
        setLastProcessTime(Math.min(getLastProcessTime(), task.getLastProcessTime()));
        Loggers.PUSH.info("[PUSH] Task merge for {}", service);
    }
    
    public Service getService() {
        return service;
    }
    
    public boolean isPushToAll() {
        return pushToAll;
    }
    
    public Set<String> getTargetClients() {
        return targetClients;
    }
}
