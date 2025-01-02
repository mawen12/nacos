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

package com.alibaba.nacos.common.notify;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract class for event.
 *
 * 代表事件的抽象类
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
@SuppressWarnings({"PMD.AbstractClassShouldStartWithAbstractNamingRule"})
public abstract class Event implements Serializable {
    
    private static final long serialVersionUID = -3731383194964997493L;

    /**
     * 原子变量，序列号
     */
    private static final AtomicLong SEQUENCE = new AtomicLong(0);
    
    private final long sequence = SEQUENCE.getAndIncrement();
    
    /**
     * Event sequence number, which can be used to handle the sequence of events.
     *
     * 事件序列号，用于处理事件的顺序
     *
     * @return sequence num, It's best to make sure it's monotone.
     */
    public long sequence() {
        return sequence;
    }
    
    /**
     * Event scope.
     *
     * 事件范围
     *
     * @return event scope, return null if for all scope
     */
    public String scope() {
        return null;
    }
    
    /**
     * Whether is plugin event. If so, the event can be dropped when no publish and subscriber without any hint. Default
     * false
     *
     * 是否为插件事件，如果是，当没有任何发布和订阅者且没有任何提示时，事件可以丢弃
     *
     * @return {@code true} if is plugin event, otherwise {@code false}
     */
    public boolean isPluginEvent() {
        return false;
    }
}

