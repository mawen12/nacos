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

package com.alibaba.nacos.client.naming.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 实例选择器
 *
 * @author alibaba
 */
public class Chooser<K, T> {

    private final K uniqueKey;

    private volatile Ref<T> ref;
    
    public Chooser(K uniqueKey) {
        this(uniqueKey, new ArrayList<>());
    }
    
    public Chooser(K uniqueKey, List<Pair<T>> pairs) {
        Ref<T> ref = new Ref<>(pairs);
        ref.refresh();
        this.uniqueKey = uniqueKey;
        this.ref = ref;
    }
    
    /**
     * Random get one item.
     *
     * @return item
     */
    public T random() {
        List<T> items = ref.items;
        if (items.size() == 0) {
            return null;
        }
        if (items.size() == 1) {
            return items.get(0);
        }
        return items.get(ThreadLocalRandom.current().nextInt(items.size()));
    }
    
    /**
     * 根据权重随机返回
     *
     * @return item
     */
    public T randomWithWeight() {
        Ref<T> ref = this.ref;
        /**
         * 获取[0, 1)之间的随机数
         */
        double random = ThreadLocalRandom.current().nextDouble(0, 1);
        /**
         * 从权重中二分查找
         */
        int index = Arrays.binarySearch(ref.weights, random);
        if (index < 0) {
            /**
             * 如果没有找到，index返回-1，则将index职位0
             */
            index = -index - 1;
        } else {
            /**
             * 如果找到了，则取对应index的元素
             */
            return ref.items.get(index);
        }

        /**
         * 如果index在指定范围内，且随机数也小于index对应的权重，则取该元素
         */
        if (index < ref.weights.length) {
            if (random < ref.weights[index]) {
                return ref.items.get(index);
            }
        }

        /**
         * 如果没有任何可选权重，代表要么是没有任何实例，要么是所有实例的权重都低于0，或均不是健康的
         */
        if (ref.weights.length == 0) {
            throw new IllegalStateException("Cumulative Weight wrong , the array length is equal to 0.");
        }

        /**
         * 兜底方案，选择最后一个元素
         */
        return ref.items.get(ref.items.size() - 1);
    }
    
    public K getUniqueKey() {
        return uniqueKey;
    }
    
    public Ref<T> getRef() {
        return ref;
    }
    
    /**
     * 刷新元素
     *
     * @param itemsWithWeight items with weight
     */
    public void refresh(List<Pair<T>> itemsWithWeight) {
        /**
         * 构造新的Ref
         */
        Ref<T> newRef = new Ref<>(itemsWithWeight);
        /**
         * 在内部重新计算权重
         */
        newRef.refresh();
        /**
         * 刷新通用轮询器
         */
        newRef.poller = this.ref.poller.refresh(newRef.items);
        this.ref = newRef;
    }
    
    public class Ref<T> {
        /**
         * List<Pair<实例, 实例的权重>>
         */
        private List<Pair<T>> itemsWithWeight = new ArrayList<>();

        /**
         * List<实例>
         */
        private final List<T> items = new ArrayList<>();

        /**
         * 轮询器
         */
        private Poller<T> poller = new GenericPoller<>(items);

        /**
         * 权重数组
         */
        private double[] weights;
        
        public Ref(List<Pair<T>> itemsWithWeight) {
            if (itemsWithWeight != null) {
                this.itemsWithWeight = itemsWithWeight;
            }
        }
        
        /**
         * Refresh.
         */
        public void refresh() {
            double originWeightSum = 0;
            int size = 0;
            /**
             * 遍历元素
             */
            for (Pair<T> item : itemsWithWeight) {
                
                double weight = item.weight();
                //ignore item which weight is zero.see test_randomWithWeight_weight0 in ChooserTest
                /**
                 * 忽略权重<=0的实例
                 */
                if (weight <= 0) {
                    continue;
                }
                
                items.add(item.item());
                /**
                 * 权重最大值为1000
                 */
                if (Double.isInfinite(weight)) {
                    weight = 10000.0D;
                }
                /**
                 * 对于非数字的权重，修改为默认值1
                 */
                if (Double.isNaN(weight)) {
                    weight = 1.0D;
                }
                /**
                 * 将权重累加
                 */
                originWeightSum += weight;
                size++;
            }
            
            weights = new double[size];
            /**
             * 精确权重
             */
            double exactWeight;
            double randomRange = 0D;
            int index = 0;
            for (Pair<T> item : itemsWithWeight) {
                double singleWeight = item.weight();
                //ignore item which weight is zero.see test_randomWithWeight_weight0 in ChooserTest
                if (singleWeight <= 0) {
                    continue;
                }

                /**
                 * 使用单个权重/总权重，获取单个权重的占比
                 */
                exactWeight = singleWeight / originWeightSum;
                /**
                 * 将占比值+前一个值增加，放置到
                 */
                weights[index] = randomRange + exactWeight;
                randomRange = weights[index++];
            }
            
            double doublePrecisionDelta = 0.0001;
            
            if (index == 0 || (Math.abs(weights[index - 1] - 1) < doublePrecisionDelta)) {
                return;
            }
            throw new IllegalStateException(
                    "Cumulative Weight calculate wrong , the sum of probabilities does not equals 1.");
        }
        
        @Override
        public int hashCode() {
            return itemsWithWeight.hashCode();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }
            Ref<T> otherRef = (Ref<T>) other;
            return this.itemsWithWeight.equals(otherRef.itemsWithWeight);
        }
    }
    
    @Override
    public int hashCode() {
        return uniqueKey.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        
        Chooser otherChooser = (Chooser) other;
        if (this.uniqueKey == null) {
            if (otherChooser.getUniqueKey() != null) {
                return false;
            }
        } else {
            if (otherChooser.getUniqueKey() == null) {
                return false;
            } else if (!this.uniqueKey.equals(otherChooser.getUniqueKey())) {
                return false;
            }
            
        }
        return this.ref.equals(otherChooser.getRef());
    }
}
