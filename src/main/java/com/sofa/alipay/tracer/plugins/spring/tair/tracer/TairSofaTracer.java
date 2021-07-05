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
package com.sofa.alipay.tracer.plugins.spring.tair.tracer;

import com.alipay.common.tracer.core.appender.encoder.SpanEncoder;
import com.alipay.common.tracer.core.configuration.SofaTracerConfiguration;
import com.alipay.common.tracer.core.constants.ComponentNameConstants;
import com.alipay.common.tracer.core.reporter.stat.AbstractSofaTracerStatisticReporter;
import com.alipay.common.tracer.core.span.SofaTracerSpan;
import com.alipay.common.tracer.core.tracer.AbstractClientTracer;
import com.sofa.alipay.tracer.plugins.spring.tair.encoder.TairDigestEncoder;
import com.sofa.alipay.tracer.plugins.spring.tair.encoder.TairDigestJsonEncoder;
import com.sofa.alipay.tracer.plugins.spring.tair.enums.TairLogEnum;
import com.sofa.alipay.tracer.plugins.spring.tair.reporter.TairStatJsonReporter;
import com.sofa.alipay.tracer.plugins.spring.tair.reporter.TairStatReporter;

/**
 * @ClassName: TairSofaTracer
 * @Description:
 * @Author: zhuangxinjie
 * @Date: 2021/6/11 2:28 下午
 */
public class TairSofaTracer extends AbstractClientTracer {
    public static final String             TAIR           = "tair";
    private volatile static TairSofaTracer tairSofaTracer = null;

    public static TairSofaTracer getTairSofaTracerSingleton() {
        if (tairSofaTracer == null) {
            synchronized (TairSofaTracer.class) {
                if (tairSofaTracer == null) {
                    tairSofaTracer = new TairSofaTracer(TAIR);
                }
            }
        }
        return tairSofaTracer;
    }

    public TairSofaTracer(String tracerType) {
        super(tracerType);
    }

    //TODO 添加 tair
    @Override
    protected String getClientDigestReporterLogName() {
        return TairLogEnum.TAIR_DIGEST.getDefaultLogName();
    }

    @Override
    protected String getClientDigestReporterRollingKey() {
        return TairLogEnum.TAIR_DIGEST.getRollingKey();
    }

    @Override
    protected String getClientDigestReporterLogNameKey() {
        return TairLogEnum.TAIR_DIGEST.getLogNameKey();
    }

    @Override
    protected SpanEncoder<SofaTracerSpan> getClientDigestEncoder() {
        //default json output
        if (SofaTracerConfiguration.isJsonOutput()) {
            return new TairDigestJsonEncoder();
        } else {
            return new TairDigestEncoder();
        }
    }

    @Override
    protected AbstractSofaTracerStatisticReporter generateClientStatReporter() {
        TairLogEnum tairLogEnum = TairLogEnum.TAIR_STAT;
        String statLog = tairLogEnum.getDefaultLogName();
        String statRollingPolicy = SofaTracerConfiguration.getRollingPolicy(tairLogEnum
            .getRollingKey());
        String statLogReserveConfig = SofaTracerConfiguration.getLogReserveConfig(tairLogEnum
            .getLogNameKey());
        //stat
        return this.getTairClientStatReporter(statLog, statRollingPolicy, statLogReserveConfig);
    }

    protected AbstractSofaTracerStatisticReporter getTairClientStatReporter(String statTracerName,
                                                                            String statRollingPolicy,
                                                                            String statLogReserveConfig) {

        if (SofaTracerConfiguration.isJsonOutput()) {
            return new TairStatJsonReporter(statTracerName, statRollingPolicy, statLogReserveConfig);
        } else {
            return new TairStatReporter(statTracerName, statRollingPolicy, statLogReserveConfig);
        }

    }
}
