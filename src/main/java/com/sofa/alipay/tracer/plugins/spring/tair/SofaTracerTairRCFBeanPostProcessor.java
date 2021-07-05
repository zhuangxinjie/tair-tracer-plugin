/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sofa.alipay.tracer.plugins.spring.tair;

import com.aliyun.tair.springdata.TairJedisConnectionFactory;
import com.sofa.alipay.tracer.plugins.spring.tair.common.TairActionWrapperHelper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * @ClassName: SofaTracerRCFBeanPostProcessor
 * @Description:  JedisCluster
 * @Author: zhuangxinjie
 * @Date: 2021/6/18 4:53 下午
 */
public class SofaTracerTairRCFBeanPostProcessor implements BeanPostProcessor {
    private final TairActionWrapperHelper actionWrapper;

    public SofaTracerTairRCFBeanPostProcessor(TairActionWrapperHelper actionWrapper) {
        this.actionWrapper = actionWrapper;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName)
                                                                              throws BeansException {
        if (bean instanceof TairJedisConnectionFactory) {
            bean = new TracingTairJedisConnectionFactory(actionWrapper,
                (TairJedisConnectionFactory) bean);
        }
        return bean;
    }
}
