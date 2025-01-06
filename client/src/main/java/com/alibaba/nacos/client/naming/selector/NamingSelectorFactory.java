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

package com.alibaba.nacos.client.naming.selector;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.selector.NamingSelector;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * 简单工厂设计模式。
 * 实例选择器工厂，提供创建多种实例选择器的功能。
 * <ul>
 *     <li>基于实例集群：{@link ClusterSelector}</li>
 *     <li>基于实例ip：{@link #newIpSelector(String)}</li>
 *     <li>基于元数据：{@link #newMetadataSelector(Map)}</li>
 * </ul>
 *
 * @author lideyou
 */
public final class NamingSelectorFactory {

    /**
     * 默认过滤器，不进行任何过滤
     */
    public static final NamingSelector EMPTY_SELECTOR = context -> context::getInstances;

    /**
     * 健康度过滤器，基于实例的健康度的过滤器
     */
    public static final NamingSelector HEALTHY_SELECTOR = new DefaultNamingSelector(Instance::isHealthy);

    /**
     * 基于集群名称的过滤器
     */
    private static class ClusterSelector extends DefaultNamingSelector {

        private final String clusterString;

        public ClusterSelector(Predicate<Instance> filter, String clusterString) {
            super(filter);
            this.clusterString = clusterString;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClusterSelector that = (ClusterSelector) o;
            return Objects.equals(this.clusterString, that.clusterString);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.clusterString);
        }
    }

    private NamingSelectorFactory() {
    }

    /**
     * 创建基于给定集群列表的实例过滤器
     *
     * @param clusters target cluster
     * @return cluster selector
     */
    public static NamingSelector newClusterSelector(Collection<String> clusters) {
        if (CollectionUtils.isNotEmpty(clusters)) {
            final Set<String> set = new HashSet<>(clusters);
            /**
             * 构造基于集群的过滤实例条件
             */
            Predicate<Instance> filter = instance -> set.contains(instance.getClusterName());
            /**
             * 转换为字符串，格式为cluster1,cluster2,...,clustern
             */
            String clusterString = getUniqueClusterString(clusters);
            /**
             * 使用过滤条件和集群字符串 -> ClusterSelector
             */
            return new ClusterSelector(filter, clusterString);
        } else {
            /**
             * 没有指定集群，则直接使用空选择器，对结果不做任何过滤
             */
            return EMPTY_SELECTOR;
        }
    }

    /**
     * 创建基于给定ip正则表达式的实例过滤器
     *
     * @param regex regular expression of IP
     * @return IP selector
     */
    public static NamingSelector newIpSelector(String regex) {
        /**
         * ip正则表达式不允许为空
         */
        if (regex == null) {
            throw new IllegalArgumentException("The parameter 'regex' cannot be null.");
        }
        return new DefaultNamingSelector(instance -> Pattern.matches(regex, instance.getIp()));
    }

    /**
     * 创建基于给定元数据的实例过滤器
     *
     * @param metadata metadata that needs to be matched
     * @return metadata selector
     */
    public static NamingSelector newMetadataSelector(Map<String, String> metadata) {
        return newMetadataSelector(metadata, false);
    }

    /**
     * 创建基于给定元数据的实例过滤器
     *
     * @param metadata target metadata
     * @param isAny    true if any of the metadata needs to be matched, false if all the metadata need to be matched.
     * @return metadata selector
     */
    public static NamingSelector newMetadataSelector(Map<String, String> metadata, boolean isAny) {
        /**
         * 元信息不允许为空
         */
        if (metadata == null) {
            throw new IllegalArgumentException("The parameter 'metadata' cannot be null.");
        }
        /**
         * 基于元数据大小创建过滤器
         */
        Predicate<Instance> filter = instance -> instance.getMetadata().size() >= metadata.size();

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            /**
             * 使用元数据目标值过滤实例
             */
            Predicate<Instance> nextFilter = instance -> {
                Map<String, String> map = instance.getMetadata();
                return Objects.equals(map.get(entry.getKey()), entry.getValue());
            };
            /**
             * 根据过滤模式，取and或or
             */
            if (isAny) {
                filter = filter.or(nextFilter);
            } else {
                filter = filter.and(nextFilter);
            }
        }
        return new DefaultNamingSelector(filter);
    }

    public static String getUniqueClusterString(Collection<String> cluster) {
        TreeSet<String> treeSet = new TreeSet<>(cluster);
        return StringUtils.join(treeSet, ",");
    }

}
