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

package org.apache.dolphinscheduler.server.master.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.StateEvent;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.enums.TimeoutFlag;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.runner.task.CommonTaskProcessor;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class FailoverRecoveryRegressionTest {
    static void set(Object target, String name, Object value) throws Exception {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                f.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    static Object get(Object target, String name) throws Exception {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(target);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    static Object call(Object target, Class<?> owner, String name, Class<?>[] types, Object... args)
            throws Exception {
        Method m = owner.getDeclaredMethod(name, types);
        m.setAccessible(true);
        return m.invoke(target, args);
    }

    static TaskInstance task(ExecutionStatus state) {
        TaskInstance t = new TaskInstance();
        t.setId(101);
        t.setTaskCode(11L);
        t.setProcessInstanceId(201);
        t.setTaskDefinitionVersion(1);
        t.setTaskType("SHELL");
        t.setState(state);
        t.setFlag(Flag.YES);
        return t;
    }

    static ProcessInstance process() {
        ProcessInstance p = new ProcessInstance();
        p.setId(201);
        p.setCommandType(CommandType.SCHEDULER);
        p.setState(ExecutionStatus.RUNNING_EXECUTION);
        return p;
    }

    private static class RetryReached extends RuntimeException {}

    private StateEvent faultEvent() {
        StateEvent event = new StateEvent();
        event.setProcessInstanceId(201);
        event.setTaskInstanceId(101);
        event.setType(StateEventType.TASK_STATE_CHANGE);
        event.setExecutionStatus(ExecutionStatus.NEED_FAULT_TOLERANCE);
        return event;
    }

    private WorkflowExecuteThread workflow(ProcessService service) throws Exception {
        WorkflowExecuteThread w =
                spy(
                        new WorkflowExecuteThread(
                                process(),
                                service,
                                null,
                                null,
                                new MasterConfig(),
                                mock(StateWheelExecuteThread.class)));
        ((Map<Integer, TaskInstance>) get(w, "taskInstanceMap"))
                .put(101, task(ExecutionStatus.RUNNING_EXECUTION));
        ((Map<Long, Integer>) get(w, "validTaskMap")).put(11L, 101);
        return w;
    }

    @Test
    public void failoverRefreshesStateBeforeEnteringRetry() throws Exception {
        ProcessService service = mock(ProcessService.class);
        WorkflowExecuteThread w = workflow(service);
        TaskInstance latest = task(ExecutionStatus.NEED_FAULT_TOLERANCE);
        when(service.findTaskInstanceById(101)).thenReturn(latest);
        // Stop at the retry allocation boundary; no worker dispatch is exercised here.
        doThrow(new RetryReached()).when(w).cloneRetryTaskInstance(latest);
        try {
            call(
                    w,
                    WorkflowExecuteThread.class,
                    "taskStateChangeHandler",
                    new Class<?>[] {StateEvent.class},
                    faultEvent());
            fail("failover event did not reach retry allocation");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof RetryReached);
        }
        assertSame(latest, w.getTaskInstance(101));
        verify(service).packageTaskInstance(latest, w.getProcessInstance());
        verify(w).cloneRetryTaskInstance(latest);
    }

    @Test
    public void oldAttemptEventDoesNotTouchNewAttempt() throws Exception {
        ProcessService service = mock(ProcessService.class);
        WorkflowExecuteThread w = workflow(service);
        ((Map<Long, Integer>) get(w, "validTaskMap")).put(11L, 102);
        assertEquals(
                true,
                call(
                        w,
                        WorkflowExecuteThread.class,
                        "taskStateChangeHandler",
                        new Class<?>[] {StateEvent.class},
                        faultEvent()));
        verify(service, never()).findTaskInstanceById(anyInt());
        verify(w, never()).cloneRetryTaskInstance(any());
    }

    @Test
    public void duplicateFaultEventDoesNotAllocateAnotherQueuedRetry() throws Exception {
        ProcessService service = mock(ProcessService.class);
        WorkflowExecuteThread w = workflow(service);
        TaskInstance pending = task(ExecutionStatus.NEED_FAULT_TOLERANCE);
        pending.setId(0);
        ((org.apache.dolphinscheduler.service.queue.PeerTaskInstancePriorityQueue)
                        get(w, "readyToSubmitTaskQueue"))
                .put(pending);
        assertEquals(
                true,
                call(
                        w,
                        WorkflowExecuteThread.class,
                        "taskStateChangeHandler",
                        new Class<?>[] {StateEvent.class},
                        faultEvent()));
        verify(service, never()).findTaskInstanceById(anyInt());
        verify(w, never()).cloneRetryTaskInstance(any());
    }

    @Test
    public void stopPreservesExistingDefinitionWithoutAnotherLookup() throws Exception {
        ProcessService service = mock(ProcessService.class);
        CommonTaskProcessor processor = mock(CommonTaskProcessor.class, CALLS_REAL_METHODS);
        TaskInstance old = task(ExecutionStatus.RUNNING_EXECUTION),
                latest = task(ExecutionStatus.SUCCESS);
        TaskDefinition definition = new TaskDefinition();
        old.setTaskDefine(definition);
        set(processor, "taskInstance", old);
        set(processor, "processService", service);
        when(service.findTaskInstanceById(101)).thenReturn(latest);
        assertTrue(processor.killTask());
        assertSame(definition, processor.taskInstance().getTaskDefine());
        verify(service, never()).packageTaskInstance(any(), any());
    }

    @Test
    public void invalidatedAttemptIsNotRetried() throws Exception {
        ProcessService service = mock(ProcessService.class);
        WorkflowExecuteThread w = workflow(service);
        TaskInstance latest = task(ExecutionStatus.NEED_FAULT_TOLERANCE);
        latest.setFlag(Flag.NO);
        when(service.findTaskInstanceById(101)).thenReturn(latest);
        assertEquals(
                true,
                call(
                        w,
                        WorkflowExecuteThread.class,
                        "taskStateChangeHandler",
                        new Class<?>[] {StateEvent.class},
                        faultEvent()));
        verify(w, never()).cloneRetryTaskInstance(any());
    }

    @Test
    public void staleFaultEventDoesNotOverwriteAlreadyCompletedSuccess() throws Exception {
        ProcessService service = mock(ProcessService.class);
        WorkflowExecuteThread w = workflow(service);
        TaskInstance latest = task(ExecutionStatus.SUCCESS);
        when(service.findTaskInstanceById(101)).thenReturn(latest);
        ((Map<Long, Integer>) get(w, "completeTaskMap")).put(11L, 101);
        assertEquals(
                true,
                call(
                        w,
                        WorkflowExecuteThread.class,
                        "taskStateChangeHandler",
                        new Class<?>[] {StateEvent.class},
                        faultEvent()));
        assertEquals(ExecutionStatus.SUCCESS, w.getTaskInstance(101).getState());
        verify(w, never()).cloneRetryTaskInstance(any());
    }

    @Test
    public void stopKeepsHydratedTaskContext() throws Exception {
        ProcessService service = mock(ProcessService.class);
        CommonTaskProcessor processor = mock(CommonTaskProcessor.class, CALLS_REAL_METHODS);
        TaskInstance old = task(ExecutionStatus.RUNNING_EXECUTION),
                latest = task(ExecutionStatus.SUCCESS);
        TaskDefinition definition = new TaskDefinition();
        definition.setTimeoutFlag(TimeoutFlag.OPEN);
        ProcessInstance p = process();
        set(processor, "taskInstance", old);
        set(processor, "processInstance", p);
        set(processor, "processService", service);
        when(service.findTaskInstanceById(101)).thenReturn(latest);
        doAnswer(
                        invocation -> {
                            latest.setTaskDefine(definition);
                            latest.setProcessInstance(p);
                            return null;
                        })
                .when(service)
                .packageTaskInstance(latest, p);
        assertTrue(processor.killTask());
        assertSame(definition, processor.taskInstance().getTaskDefine());
        assertSame(p, processor.taskInstance().getProcessInstance());
    }

    @Test
    public void stopMissingRowDoesNotNullOutCachedTask() throws Exception {
        ProcessService service = mock(ProcessService.class);
        CommonTaskProcessor processor = mock(CommonTaskProcessor.class, CALLS_REAL_METHODS);
        TaskInstance old = task(ExecutionStatus.RUNNING_EXECUTION);
        set(processor, "taskInstance", old);
        set(processor, "processService", service);
        set(processor, "logger", org.slf4j.LoggerFactory.getLogger(getClass()));
        assertFalse(processor.killTask());
        assertSame(old, processor.taskInstance());
    }

    @Test
    public void badTimeoutEntryCannotStarveHealthyEntry() throws Exception {
        assertTimeoutIsolation(false);
    }

    @Test
    public void databaseExceptionCannotStarveHealthyTimeoutEntry() throws Exception {
        assertTimeoutIsolation(true);
    }

    private void assertTimeoutIsolation(boolean databaseThrows) throws Exception {
        StateWheelExecuteThread wheel = new StateWheelExecuteThread();
        ProcessService service = mock(ProcessService.class);
        ProcessInstanceExecCacheManager cache = mock(ProcessInstanceExecCacheManager.class);
        WorkflowExecuteThread w = mock(WorkflowExecuteThread.class);
        WorkflowExecuteThreadPool pool = mock(WorkflowExecuteThreadPool.class);
        set(wheel, "processService", service);
        set(wheel, "processInstanceExecCacheManager", cache);
        set(wheel, "workflowExecuteThreadPool", pool);
        when(cache.getByProcessInstanceId(201)).thenReturn(w);
        TaskDefinition definition = new TaskDefinition();
        definition.setTimeoutFlag(TimeoutFlag.OPEN);
        definition.setTimeout(1);
        TaskInstance bad = task(ExecutionStatus.RUNNING_EXECUTION),
                good = task(ExecutionStatus.RUNNING_EXECUTION);
        bad.setTaskDefine(definition);
        good.setTaskDefine(definition);
        good.setTaskCode(22L);
        good.setId(102);
        good.setStartTime(new Date(System.currentTimeMillis() - 120000));
        when(w.getActiveTaskInstanceByTaskCode(11L)).thenReturn(bad);
        when(w.getActiveTaskInstanceByTaskCode(22L)).thenReturn(good);
        wheel.addTask4TimeoutCheck(process(), bad);
        wheel.addTask4TimeoutCheck(process(), good);
        bad.setTaskDefine(null);
        if (databaseThrows)
            when(service.findTaskDefinition(11L, 1))
                    .thenThrow(new IllegalStateException("test lookup failure"));
        call(wheel, StateWheelExecuteThread.class, "checkTask4Timeout", new Class<?>[] {});
        org.mockito.ArgumentCaptor<StateEvent> event =
                org.mockito.ArgumentCaptor.forClass(StateEvent.class);
        verify(pool).submitStateEvent(event.capture());
        assertEquals(102, event.getValue().getTaskInstanceId());
        assertEquals(StateEventType.TASK_TIMEOUT, event.getValue().getType());
        assertEquals(1, ((Collection<?>) get(wheel, "taskInstanceTimeoutCheckList")).size());
    }
}
