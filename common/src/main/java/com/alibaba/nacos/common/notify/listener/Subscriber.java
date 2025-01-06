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

package com.alibaba.nacos.common.notify.listener;

import com.alibaba.nacos.common.notify.Event;

import java.util.concurrent.Executor;

/**
 * 代表订阅者接口的抽象订阅类
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class Subscriber<T extends Event> {
    
    /**
     * 事件回调
     *
     * @param event {@link Event}
     */
    public abstract void onEvent(T event);
    
    /**
     * 订阅者类型
     *
     * @return Class which extends {@link Event}
     */
    public abstract Class<? extends Event> subscribeType();
    
    /**
     * 由订阅者提供的执行器，确定监听是同步还是异步的
     *
     * @return {@link Executor}
     */
    public Executor executor() {
        return null;
    }
    
    /**
     * 是否忽略过期事件
     *
     * @return default value is {@link Boolean#FALSE}
     */
    public boolean ignoreExpireEvent() {
        return false;
    }
    
    /**
     * Whether the event's scope matches current subscriber. Default implementation is all scopes matched.
     * If you override this method, it better to override related {@link com.alibaba.nacos.common.notify.Event#scope()}.
     *
     * 事件范围是否匹配当前订阅者，默认是全部匹配。
     *
     * @param event {@link Event}
     * @return Whether the event's scope matches current subscriber
     */
    public boolean scopeMatches(T event) {
        return true;
    }
}
