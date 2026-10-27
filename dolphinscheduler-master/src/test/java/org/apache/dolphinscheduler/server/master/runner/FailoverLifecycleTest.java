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

import static org.apache.dolphinscheduler.server.master.runner.FailoverRecoveryRegressionTest.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.*;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.server.master.runner.task.*;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.dolphinscheduler.service.registry.RegistryClient;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import java.util.*;

public class FailoverLifecycleTest {
    public static class RecordingProcessor implements ITaskProcessor {
        static final List<TaskInstance> submitted = new ArrayList<>();
        private TaskInstance task;

        public RecordingProcessor() {}

        public void init(TaskInstance t, ProcessInstance p) {
            task = t;
        }

        public boolean action(TaskAction action) {
            if (action == TaskAction.SUBMIT) {
                task.setId(1000 + submitted.size());
                task.setState(ExecutionStatus.RUNNING_EXECUTION);
                submitted.add(task);
            }
            return true;
        }

        public String getType() {
            return Constants.COMMON_TASK_TYPE;
        }

        public TaskInstance taskInstance() {
            return task;
        }
    }

    @Test
    public void cachedFailoverSubmitsOnceAndWorkflowCompletes() throws Exception {
        lifecycle(false);
    }

    @Test
    public void takeoverLoadsFaultedTaskAndWorkflowCompletes() throws Exception {
        lifecycle(true);
    }

    private void lifecycle(boolean takeover) throws Exception {
        SpringApplicationContext spring = new SpringApplicationContext();
        ApplicationContext previous = (ApplicationContext) get(spring, "applicationContext");
        spring.setApplicationContext(mock(ApplicationContext.class));
        Map<String, ITaskProcessor> saved = new HashMap<>(TaskProcessorFactory.PROCESS_MAP);
        RecordingProcessor.submitted.clear();
        TaskProcessorFactory.PROCESS_MAP.put("SHELL", new RecordingProcessor());
        try {
            ProcessService service = mock(ProcessService.class);
            ProcessInstance p = process();
            p.setRunTimes(2);
            p.setFailureStrategy(FailureStrategy.CONTINUE);
            p.setProcessDefinitionCode(100L);
            p.setProcessDefinitionVersion(1);
            p.setName("test-flow");
            p.setTaskDependType(TaskDependType.TASK_POST);
            ProcessDefinition pd = new ProcessDefinition();
            pd.setCode(100L);
            pd.setVersion(1);
            pd.setName("test-flow");
            pd.setExecutionType(ProcessExecutionTypeEnum.SERIAL_DISCARD);
            p.setProcessDefinition(pd);
            TaskDefinition td = new TaskDefinition();
            td.setCode(11L);
            td.setVersion(1);
            td.setTimeoutFlag(TimeoutFlag.CLOSE);
            TaskNode node = new TaskNode();
            node.setCode(11L);
            node.setVersion(1);
            node.setName("test-task");
            node.setType("SHELL");
            node.setParams("{}");
            DAG<String, TaskNode, TaskNodeRelation> dag = new DAG<>();
            dag.addNode("11", node);
            WorkflowExecuteThread w =
                    new WorkflowExecuteThread(
                            p,
                            service,
                            null,
                            null,
                            new MasterConfig(),
                            mock(StateWheelExecuteThread.class));
            set(w, "dag", dag);
            set(w, "processDefinition", pd);
            TaskInstance fault = task(ExecutionStatus.NEED_FAULT_TOLERANCE);
            fault.setTaskDefine(td);
            fault.setProcessDefine(pd);
            fault.setName("test-task");
            when(service.findTaskInstanceById(101)).thenReturn(fault);
            doAnswer(
                            inv -> {
                                TaskInstance t = inv.getArgument(0);
                                t.setTaskDefine(td);
                                t.setProcessDefine(pd);
                                t.setProcessInstance(p);
                                return null;
                            })
                    .when(service)
                    .packageTaskInstance(any(), any());
            if (takeover) {
                when(service.findValidTaskListByProcessId(201))
                        .thenReturn(Collections.singletonList(fault));
                call(w, WorkflowExecuteThread.class, "initTaskQueue", new Class<?>[] {});
                call(w, WorkflowExecuteThread.class, "submitStandByTask", new Class<?>[] {});
            } else {
                TaskInstance running = task(ExecutionStatus.RUNNING_EXECUTION);
                running.setTaskDefine(td);
                ((Map<Integer, TaskInstance>) get(w, "taskInstanceMap")).put(101, running);
                ((Map<Long, Integer>) get(w, "validTaskMap")).put(11L, 101);
                RecordingProcessor active = new RecordingProcessor();
                active.init(running, p);
                ((Map<Long, ITaskProcessor>) get(w, "activeTaskProcessorMaps")).put(11L, active);
                call(
                        w,
                        WorkflowExecuteThread.class,
                        "taskStateChangeHandler",
                        new Class<?>[] {StateEvent.class},
                        event(101, ExecutionStatus.NEED_FAULT_TOLERANCE));
            }
            assertEquals(1, RecordingProcessor.submitted.size());
            TaskInstance retry = RecordingProcessor.submitted.get(0);
            assertEquals(Flag.NO, fault.getFlag());
            assertEquals(ExecutionStatus.RUNNING_EXECUTION, retry.getState());
            assertEquals(
                    Integer.valueOf(retry.getId()),
                    ((Map<Long, Integer>) get(w, "validTaskMap")).get(11L));
            // A duplicated old event cannot allocate another attempt.
            call(
                    w,
                    WorkflowExecuteThread.class,
                    "taskStateChangeHandler",
                    new Class<?>[] {StateEvent.class},
                    event(101, ExecutionStatus.NEED_FAULT_TOLERANCE));
            assertEquals(1, RecordingProcessor.submitted.size());
            retry.setState(ExecutionStatus.SUCCESS);
            call(
                    w,
                    WorkflowExecuteThread.class,
                    "taskStateChangeHandler",
                    new Class<?>[] {StateEvent.class},
                    event(retry.getId(), ExecutionStatus.SUCCESS));
            assertEquals(ExecutionStatus.SUCCESS, p.getState());
            assertNotNull(p.getEndTime());
            assertEquals(1, RecordingProcessor.submitted.size());
        } finally {
            TaskProcessorFactory.PROCESS_MAP.clear();
            TaskProcessorFactory.PROCESS_MAP.putAll(saved);
            spring.setApplicationContext(previous);
        }
    }

    private StateEvent event(int id, ExecutionStatus state) {
        StateEvent e = new StateEvent();
        e.setProcessInstanceId(201);
        e.setTaskInstanceId(id);
        e.setType(StateEventType.TASK_STATE_CHANGE);
        e.setExecutionStatus(state);
        return e;
    }

    @Test
    public void healthyMasterDependencyIsNotMistakenForDeadWorker() throws Exception {
        masterSideTask(false);
    }

    @Test
    public void deadMasterStillRecoversItsDependencyTask() throws Exception {
        masterSideTask(true);
    }

    private void masterSideTask(boolean dead) throws Exception {
        ProcessService service = mock(ProcessService.class);
        RegistryClient registry = mock(RegistryClient.class);
        MasterRegistryClient client = new MasterRegistryClient();
        WorkflowExecuteThreadPool pool = mock(WorkflowExecuteThreadPool.class);
        set(client, "processService", service);
        set(client, "registryClient", registry);
        set(client, "masterConfig", mock(MasterConfig.class));
        set(client, "workflowExecuteThreadPool", pool);
        Server master = new Server();
        master.setHost("127.0.0.1");
        master.setPort(5678);
        master.setCreateTime(new Date(1000));
        when(registry.getServerList(NodeType.MASTER))
                .thenReturn(dead ? Collections.emptyList() : Collections.singletonList(master));
        when(registry.getServerList(NodeType.WORKER)).thenReturn(Collections.emptyList());
        ProcessInstance p = process();
        p.setHost("127.0.0.1:5678");
        p.setRestartTime(new Date(2000));
        TaskInstance dependent = task(ExecutionStatus.RUNNING_EXECUTION);
        dependent.setTaskType("DEPENDENT");
        dependent.setHost(p.getHost());
        when(service.queryNeedFailoverProcessInstances(p.getHost()))
                .thenReturn(Collections.singletonList(p));
        when(service.findValidTaskListByProcessId(201))
                .thenReturn(Collections.singletonList(dependent));
        client.failoverMaster(p.getHost());
        if (dead) {
            assertEquals(ExecutionStatus.NEED_FAULT_TOLERANCE, dependent.getState());
            verify(service).processNeedFailoverProcessInstances(p);
        } else {
            assertEquals(ExecutionStatus.RUNNING_EXECUTION, dependent.getState());
            verify(service, never()).saveTaskInstance(any());
            verify(service, never()).processNeedFailoverProcessInstances(any());
        }
    }
}
