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

package com.alibaba.nacos.client.config.utils;

import com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor;

/**
 * Snapshot switch.
 *
 * 快照切换
 *
 * @author Nacos
 */
public class SnapShotSwitch {
    
    /**
     * whether use local cache.
     */
    /**
     * 是否使用本地快照，标志位
     */
    private static Boolean isSnapShot = true;
    
    public static Boolean getIsSnapShot() {
        return isSnapShot;
    }

    /**
     * 更新标志位，并清除所有快照
     *
     * @param isSnapShot
     */
    public static void setIsSnapShot(Boolean isSnapShot) {
        SnapShotSwitch.isSnapShot = isSnapShot;
        LocalConfigInfoProcessor.cleanAllSnapshot();
    }
    
}
