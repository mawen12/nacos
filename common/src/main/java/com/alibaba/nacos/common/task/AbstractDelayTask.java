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

package com.alibaba.nacos.common.task;

/**
 * Abstract task which can delay and merge.
 *
 * @author huali
 * @author xiweng.yy
 */
public abstract class AbstractDelayTask implements NacosTask {
    
    /**
     * 任务两次执行的间隔，时间单位为毫秒。
     */
    private long taskInterval;
    
    /**
     * 记录任务的创建时间，如果任务上次执行失败了，则更新该时间。时间单位为毫秒。
     * 使用当前时间-该值得到的结果，便作为判断任务是否可以执行的依据
     * @see #shouldProcess()
     */
    private long lastProcessTime;
    
    /**
     * The default time interval, in milliseconds, between tasks.
     */
    protected static final long INTERVAL = 1000L;
    
    /**
     * merge task.
     *
     * @param task task
     */
    public abstract void merge(AbstractDelayTask task);
    
    public void setTaskInterval(long interval) {
        this.taskInterval = interval;
    }
    
    public long getTaskInterval() {
        return this.taskInterval;
    }
    
    public void setLastProcessTime(long lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }
    
    public long getLastProcessTime() {
        return this.lastProcessTime;
    }

    /**
     * 对于延迟任务来说，只有满足任务间隔时，才会去执行
     * @return
     */
    @Override
    public boolean shouldProcess() {
        return (System.currentTimeMillis() - this.lastProcessTime >= this.taskInterval);
    }
    
}
