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

package com.alibaba.nacos.naming.core.v2.pojo;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.utils.NamingUtils;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 代表Nacos v2的服务实例对象，使用namespace+group+name构成唯一性
 * 这就代表了微服务中的服务。一个服务可以有多个实例。
 *
 * @author xiweng.yy
 */
public class Service implements Serializable {
    
    private static final long serialVersionUID = -990509089519499344L;

    /**
     * 服务命名空间
     */
    private final String namespace;

    /**
     * 服务所属分组
     */
    private final String group;

    /**
     * 服务名称
     */
    private final String name;

    /**
     *
     */
    private final boolean ephemeral;

    /**
     * 该服务信息累计变更次数，由{@link com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.ServiceMetadataEvent}触发更新
     */
    private final AtomicLong revision;

    /**
     * 服务最后更新时间，由{@link com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent.ServiceMetadataEvent}触发更新
     */
    private long lastUpdatedTime;
    
    private Service(String namespace, String group, String name, boolean ephemeral) {
        this.namespace = namespace;
        this.group = group;
        this.name = name;
        this.ephemeral = ephemeral;
        revision = new AtomicLong();
        lastUpdatedTime = System.currentTimeMillis();
    }
    
    public static Service newService(String namespace, String group, String name) {
        return newService(namespace, group, name, true);
    }
    
    public static Service newService(String namespace, String group, String name, boolean ephemeral) {
        return new Service(namespace, group, name, ephemeral);
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
    
    public boolean isEphemeral() {
        return ephemeral;
    }
    
    public long getRevision() {
        return revision.get();
    }
    
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }
    
    public void renewUpdateTime() {
        lastUpdatedTime = System.currentTimeMillis();
    }
    
    public void incrementRevision() {
        revision.incrementAndGet();
    }

    /**
     * 返回带有分组的服务名称，格式为：{@code group@@name}
     * @return
     */
    public String getGroupedServiceName() {
        return NamingUtils.getGroupedName(name, group);
    }

    /**
     * 返回带有命名空间和分组的服务名称，格式为：{@code namespace@@group@@name}
     * @return
     */
    public String getNameSpaceGroupedServiceName() {
        //do not String.intern
        return namespace + Constants.SERVICE_INFO_SPLITER + NamingUtils.getGroupedName(name, group);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Service)) {
            return false;
        }
        Service service = (Service) o;
        return namespace.equals(service.namespace) && group.equals(service.group) && name.equals(service.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(namespace, group, name);
    }
    
    @Override
    public String toString() {
        return "Service{" + "namespace='" + namespace + '\'' + ", group='" + group + '\'' + ", name='" + name + '\''
                + ", ephemeral=" + ephemeral + ", revision=" + revision + '}';
    }
}
