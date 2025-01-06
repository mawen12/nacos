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

package com.alibaba.nacos.common.trace.event;

import com.alibaba.nacos.common.notify.Event;

/**
 * 跟踪事件
 *
 * @author yanda
 * @see com.alibaba.nacos.core.trace.NacosCombinedTraceSubscriber 事件订阅者
 */
public class TraceEvent extends Event {

    private static final long serialVersionUID = -3065900892505697062L;

    /**
     * 事件类型
     * <pre>
     * +------+--------------------------------+
     * |  含义  |               值                |
     * +------+--------------------------------+
     * | 实例注册 | REGISTER_INSTANCE_TRACE_EVENT  |
     * | 实例注销 | DEREGISTER_SERVICE_TRACE_EVENT |
     * | 服务注册 | REGISTER_SERVICE_TRACE_EVENT   |
     * | 服务注销 | DEREGISTER_SERVICE_TRACE_EVENT |
     * +------+--------------------------------+
     * </pre>
     */
    private final String type;

    /**
     * 事件创建的时间
     */
    private final long eventTime;

    /**
     * 事件发生的命名空间，对于注册中心而言，就是实例所在的命名空间
     */
    private final String namespace;

    /**
     * 事件发生的分组，对于注册中心而言，就是实例所在分组
     */
    private final String group;

    /**
     * 事件的名称，对于注册中心而言，就是实例名称
     */
    private final String name;

    public String getType() {
        return type;
    }

    public long getEventTime() {
        return eventTime;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getGroup() {
        return group;
    }

    public String getName() {
        return name;
    }

    public TraceEvent(String eventType, long eventTime, String namespace, String group, String name) {
        this.type = eventType;
        this.eventTime = eventTime;
        this.namespace = namespace;
        this.group = group;
        this.name = name;
    }
}
