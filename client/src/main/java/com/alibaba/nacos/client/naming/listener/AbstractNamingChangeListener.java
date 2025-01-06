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

package com.alibaba.nacos.client.naming.listener;

import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.Event;

/**
 * 用于{@link NamingChangeEvent}的监听器
 *
 * @author lideyou
 */
public abstract class AbstractNamingChangeListener extends AbstractEventListener {
    
    @Override
    public final void onEvent(Event event) {
        /**
         * 仅处理{@link NamingChangeEvent}事件
         */
        if (event instanceof NamingChangeEvent) {
            /**
             * 事件处理委托给{@link #onChange(NamingChangeEvent)}方法
             */
            onChange((NamingChangeEvent) event);
        }
    }
    
    /**
     * Callback when instances have changed.
     *
     * @param event NamingChangeEvent
     */
    public abstract void onChange(NamingChangeEvent event);
}
