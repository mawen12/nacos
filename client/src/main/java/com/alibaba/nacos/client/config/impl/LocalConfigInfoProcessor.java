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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.client.utils.ConcurrentDiskUtil;
import com.alibaba.nacos.client.config.utils.JvmUtil;
import com.alibaba.nacos.client.config.utils.SnapShotSwitch;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.alibaba.nacos.client.utils.ParamUtil.simplyEnvNameIfOverLimit;

/**
 * Local Disaster Recovery Directory Tool.
 *
 * 本地灾难恢复目录工具。用于管理本地目中配置的故障恢复文件。
 * 默认存储路径为：
 *  ${user.home}/nacos/config/${serverName}_nacos/data/config-data/DEFAULT_GROUP/${dataId}
 *
 * 特定命名空间的默认存储路径为：
 *  ${user.home}/nacos/config/${serverName}_nacos/data/config-data-tenant/${tenant}/DEFAULT_GROUP/#{dataId}
 *
 * 默认快照存储路径为：
 *  ${user.home}/nacos/config/${serverName}_nacos/snapshot/DEFAULT_GROUP/${dataId}
 *
 * 特定命名空间的默认存储路径为：
 *  ${user.home}/nacos/config/${serverName}_nacos/snapshot-tenant/${tenant}/DEFAULT_GROUP/${dataId}
 *
 * @author Nacos
 */
public class LocalConfigInfoProcessor {
    
    private static final Logger LOGGER = LogUtils.logger(LocalConfigInfoProcessor.class);
    
    public static final String LOCAL_SNAPSHOT_PATH;
    
    private static final String SUFFIX = "_nacos";
    
    private static final String ENV_CHILD = "snapshot";
    
    private static final String FAILOVER_FILE_CHILD_1 = "data";
    
    private static final String FAILOVER_FILE_CHILD_2 = "config-data";
    
    private static final String FAILOVER_FILE_CHILD_3 = "config-data-tenant";
    
    private static final String SNAPSHOT_FILE_CHILD_1 = "snapshot";
    
    private static final String SNAPSHOT_FILE_CHILD_2 = "snapshot-tenant";
    
    static {
        /**
         * 解析本地快照路径，从 ENV(JM.SNAPSHOT.PATH) -> ENV(user.home) + /nacos/config
         */
        LOCAL_SNAPSHOT_PATH = NacosClientProperties.PROTOTYPE.getProperty(com.alibaba.nacos.client.constant.Constants.SysEnv.JM_SNAPSHOT_PATH,
                NacosClientProperties.PROTOTYPE.getProperty(com.alibaba.nacos.client.constant.Constants.SysEnv.USER_HOME)) + File.separator
                + "nacos" + File.separator + "config";
        LOGGER.info("LOCAL_SNAPSHOT_PATH:{}", LOCAL_SNAPSHOT_PATH);
    }

    /**
     * 从本地灾难恢复路径中读取特定的serverName, dataId, group, tenant的配置
     *
     * @param serverName
     * @param dataId
     * @param group
     * @param tenant
     * @return
     */
    public static String getFailover(String serverName, String dataId, String group, String tenant) {
        /**
         * 获取本地故障恢复文件，如果文件不存在，则直接返回
         */
        File localPath = getFailoverFile(serverName, dataId, group, tenant);
        if (!localPath.exists() || !localPath.isFile()) {
            return null;
        }
        
        try {
            return readFile(localPath);
        } catch (IOException ioe) {
            LOGGER.error("[" + serverName + "] get failover error, " + localPath, ioe);
            return null;
        }
    }
    
    /**
     * get snapshot file content. NULL means no local file or throw exception.
     */
    public static String getSnapshot(String name, String dataId, String group, String tenant) {
        if (!SnapShotSwitch.getIsSnapShot()) {
            return null;
        }
        File file = getSnapshotFile(name, dataId, group, tenant);
        if (!file.exists() || !file.isFile()) {
            return null;
        }
        
        try {
            return readFile(file);
        } catch (IOException ioe) {
            LOGGER.error("[" + name + "]+get snapshot error, " + file, ioe);
            return null;
        }
    }
    
    protected static String readFile(File file) throws IOException {
        /**
         * 文件合法性检查
         */
        if (!file.exists() || !file.isFile()) {
            return null;
        }

        /**
         * 如果时多实例，则使用RandomAccessFile读取文件，否则使用FileInputStream读取文件
         */
        if (JvmUtil.isMultiInstance()) {
            return ConcurrentDiskUtil.getFileContent(file, Constants.ENCODE);
        } else {
            try (InputStream is = new FileInputStream(file)) {
                return IoUtils.toString(is, Constants.ENCODE);
            }
        }
    }
    
    /**
     * Save snapshot.
     *
     * @param envName env name
     * @param dataId  data id
     * @param group   group
     * @param tenant  tenant
     * @param config  config
     */
    public static void saveSnapshot(String envName, String dataId, String group, String tenant, String config) {
        if (!SnapShotSwitch.getIsSnapShot()) {
            return;
        }
        File file = getSnapshotFile(envName, dataId, group, tenant);
        if (null == config) {
            try {
                IoUtils.delete(file);
            } catch (IOException ioe) {
                LOGGER.error("[" + envName + "] delete snapshot error, " + file, ioe);
            }
        } else {
            try {
                File parentFile = file.getParentFile();
                if (!parentFile.exists()) {
                    boolean isMdOk = parentFile.mkdirs();
                    if (!isMdOk) {
                        LOGGER.error("[{}] save snapshot error", envName);
                    }
                }
                
                if (JvmUtil.isMultiInstance()) {
                    ConcurrentDiskUtil.writeFileContent(file, config, Constants.ENCODE);
                } else {
                    IoUtils.writeStringToFile(file, config, Constants.ENCODE);
                }
            } catch (IOException ioe) {
                LOGGER.error("[" + envName + "] save snapshot error, " + file, ioe);
            }
        }
    }
    
    /**
     * clear the cache files under snapshot directory.
     */
    public static void cleanAllSnapshot() {
        try {
            File rootFile = new File(LOCAL_SNAPSHOT_PATH);
            File[] files = rootFile.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            /**
             * 删除所有后缀为_nacos的文件
             */
            for (File file : files) {
                if (file.getName().endsWith(SUFFIX)) {
                    IoUtils.cleanDirectory(file);
                }
            }
        } catch (IOException ioe) {
            LOGGER.error("clean all snapshot error, " + ioe.toString(), ioe);
        }
    }
    
    /**
     * Clean snapshot.
     *
     * @param envName env name
     */
    public static void cleanEnvSnapshot(String envName) {
        File tmp = new File(LOCAL_SNAPSHOT_PATH, envName + SUFFIX);
        tmp = new File(tmp, ENV_CHILD);
        try {
            IoUtils.cleanDirectory(tmp);
            LOGGER.info("success delete {}-snapshot", envName);
        } catch (IOException e) {
            LOGGER.warn("fail delete {}-snapshot, exception: ", envName, e);
        }
    }
    
    static File getFailoverFile(String serverName, String dataId, String group, String tenant) {
        /**
         * 如果服务名称太长，则截取最大长度拼接原先的md5的16进制
         */
        serverName = simplyEnvNameIfOverLimit(serverName);
        /**
         * 定位特定服务的配置目录，即 LOCAL_SNAPSHOT_PATH/${serverName}_nacos
         */
        File tmp = new File(LOCAL_SNAPSHOT_PATH, serverName + SUFFIX);
        /**
         * 定位特定故障的配置目录，即 LOCAL_SNAPSHOT_PATH/${serverName}_nacos/data
         */
        tmp = new File(tmp, FAILOVER_FILE_CHILD_1);
        if (StringUtils.isBlank(tenant)) {
            /**
             * 当不存在特定租户时，定位目录为：LOCAL_SNAPSHOT_PATH/${serverName}_nacos/data/config-data
             */
            tmp = new File(tmp, FAILOVER_FILE_CHILD_2);
        } else {
            /**
             * 当存在租户时，定位目录为：LOCAL_SNAPSHOT_PATH/${serverName}_nacos/data/config-data-tenant
             */
            tmp = new File(tmp, FAILOVER_FILE_CHILD_3);
            /**
             * 切换到指定租户下：LOCAL_SNAPSHOT_PATH/${serverName}_nacos/data/config-data-tenant/${tenant}
             */
            tmp = new File(tmp, tenant);
        }
        /**
         * 切换到指定分组下，文件名称为dataId
         */
        return new File(new File(tmp, group), dataId);
    }
    
    static File getSnapshotFile(String envName, String dataId, String group, String tenant) {
        envName = simplyEnvNameIfOverLimit(envName);
        File tmp = new File(LOCAL_SNAPSHOT_PATH, envName + SUFFIX);
        if (StringUtils.isBlank(tenant)) {
            tmp = new File(tmp, SNAPSHOT_FILE_CHILD_1);
        } else {
            tmp = new File(tmp, SNAPSHOT_FILE_CHILD_2);
            tmp = new File(tmp, tenant);
        }
        
        return new File(new File(tmp, group), dataId);
    }
}
