/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.service.process;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ProcessExecutionTypeEnum;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.mapper.ProcessInstanceMapper;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SerialDiscardLiveParityTest {

    @Test
    public void discardRemainsStoppedWhenPreviousInstanceIsActive() throws Exception {
        assertDiscardState(Collections.singletonList(new ProcessInstance()), ExecutionStatus.STOP);
    }

    @Test
    public void submitWhenNoPreviousInstanceIsActive() throws Exception {
        assertDiscardState(Collections.emptyList(), ExecutionStatus.SUBMITTED_SUCCESS);
    }

    private void assertDiscardState(List<ProcessInstance> previous, ExecutionStatus expected) throws Exception {
        ProcessServiceImpl service = spy(new ProcessServiceImpl());
        ProcessInstanceMapper mapper = mock(ProcessInstanceMapper.class);
        Field field = ProcessServiceImpl.class.getDeclaredField("processInstanceMapper");
        field.setAccessible(true);
        field.set(service, mapper);
        ProcessInstance instance = new ProcessInstance();
        instance.setId(200);
        instance.setProcessDefinitionCode(100L);
        ProcessDefinition definition = new ProcessDefinition();
        definition.setExecutionType(ProcessExecutionTypeEnum.SERIAL_DISCARD);
        when(mapper.queryByProcessDefineCodeAndStatusAndNextId(100L, Constants.RUNNING_PROCESS_STATE, 200))
                .thenReturn(previous);
        List<ExecutionStatus> persisted = new ArrayList<>();
        doAnswer(invocation -> {
            persisted.add(((ProcessInstance) invocation.getArgument(0)).getState());
            return null;
        }).when(service).saveProcessInstance(any(ProcessInstance.class));

        service.saveSerialProcess(instance, definition);

        assertEquals(expected, instance.getState());
        assertEquals(Arrays.asList(ExecutionStatus.SERIAL_WAIT, expected), persisted);
    }
}
