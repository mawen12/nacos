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

package com.alibaba.nacos.plugin.datasource.constants;

import java.lang.reflect.Method;

/**
 * Datasource plugin common constant.
 *
 * @author xiweng.yy
 */
public class CommonConstant {

    /**
     * 是否开启SQL打印，打印由{@link com.alibaba.nacos.plugin.datasource.proxy.MapperProxy#invoke(Object, Method, Object[])}负责。
     */
    public static final String NACOS_PLUGIN_DATASOURCE_LOG = "nacos.plugin.datasource.log.enabled";
}
