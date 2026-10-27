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

import java.util.*;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.*;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.dispatch.executor.NettyExecutorManager;
import org.apache.dolphinscheduler.server.master.runner.task.*;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

public class TakeoverRecoveryTest {
  public static class ExistingWorkerProcessor implements ITaskProcessor {
    private TaskInstance task;

    public ExistingWorkerProcessor() {}

    public void init(TaskInstance t, ProcessInstance p) {
      task = t;
    }

    public boolean action(TaskAction action) {
      if (action == TaskAction.SUBMIT || action == TaskAction.DISPATCH) {
        fail("A live worker attempt must not be resubmitted");
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
  public void runningWorkerIsReattachedAndCompletes() throws Exception {
    recover(ExecutionStatus.RUNNING_EXECUTION, ExecutionStatus.SUBMITTED_SUCCESS);
  }

  @Test
  public void delayedWorkerIsReattachedWithoutRedispatch() throws Exception {
    recover(ExecutionStatus.DELAY_EXECUTION, ExecutionStatus.SUBMITTED_SUCCESS);
  }

  @Test
  public void dispatchedWorkerIsReattachedWithoutRedispatch() throws Exception {
    recover(ExecutionStatus.DISPATCH, ExecutionStatus.SUBMITTED_SUCCESS);
  }

  @Test
  public void firstRunParallelRecoveryLoadsExistingTasks() throws Exception {
    recover(ExecutionStatus.RUNNING_EXECUTION, ExecutionStatus.RUNNING_EXECUTION);
  }

  @Test
  public void alreadyCompletedSerialRecoveryFinishes() throws Exception {
    recover(ExecutionStatus.SUCCESS, ExecutionStatus.SUBMITTED_SUCCESS);
  }

  private void recover(ExecutionStatus taskState, ExecutionStatus processState) throws Exception {
    SpringApplicationContext spring = new SpringApplicationContext();
    ApplicationContext previous = (ApplicationContext) get(spring, "applicationContext");
    spring.setApplicationContext(mock(ApplicationContext.class));
    Map<String, ITaskProcessor> saved = new HashMap<>(TaskProcessorFactory.PROCESS_MAP);
    TaskProcessorFactory.PROCESS_MAP.put("SHELL", new ExistingWorkerProcessor());
    try {
      ProcessService service = mock(ProcessService.class);
      NettyExecutorManager netty = mock(NettyExecutorManager.class);
      StateWheelExecuteThread wheel = mock(StateWheelExecuteThread.class);
      ProcessInstance p = process();
      p.setState(processState);
      p.setRunTimes(1);
      p.setRecovery(Flag.YES);
      p.setCommandType(CommandType.RECOVER_TOLERANCE_FAULT_PROCESS);
      p.setFailureStrategy(FailureStrategy.CONTINUE);
      p.setTaskDependType(TaskDependType.TASK_POST);
      ProcessDefinition pd = new ProcessDefinition();
      pd.setCode(100L);
      pd.setVersion(1);
      pd.setExecutionType(ProcessExecutionTypeEnum.SERIAL_DISCARD);
      p.setProcessDefinition(pd);
      TaskDefinition td = new TaskDefinition();
      td.setCode(11L);
      td.setVersion(1);
      td.setTimeoutFlag(TimeoutFlag.CLOSE);
      TaskInstance t = task(taskState);
      t.setHost("127.0.0.1:1234");
      t.setTaskDefine(td);
      t.setTaskGroupId(9);
      TaskNode node = new TaskNode();
      node.setCode(11L);
      node.setVersion(1);
      node.setType("SHELL");
      node.setParams("{}");
      DAG<String, TaskNode, TaskNodeRelation> dag = new DAG<>();
      dag.addNode("11", node);
      WorkflowExecuteThread w =
          new WorkflowExecuteThread(p, service, netty, null, new MasterConfig(), wheel);
      set(w, "dag", dag);
      set(w, "processDefinition", pd);
      when(service.findValidTaskListByProcessId(201)).thenReturn(Collections.singletonList(t));
      call(w, WorkflowExecuteThread.class, "initTaskQueue", new Class<?>[] {});
      verify(service).findValidTaskListByProcessId(201);
      verify(service, never()).submitTaskWithRetry(any(), any(), anyInt(), anyInt());
      verify(service, never())
          .acquireTaskGroup(anyInt(), anyString(), anyInt(), anyInt(), anyInt());
      assertEquals(Integer.valueOf(101), ((Map<Long, Integer>) get(w, "validTaskMap")).get(11L));
      if (taskState != ExecutionStatus.SUCCESS) {
        assertSame(t, w.getActiveTaskInstanceByTaskCode(11L));
        verify(netty).doExecute(any(), any());
        verify(wheel).addTask4TimeoutCheck(p, t);
        verify(wheel).addTask4StateCheck(p, t);
        call(
            w,
            WorkflowExecuteThread.class,
            "submitPostNode",
            new Class<?>[] {String.class},
            (Object) null);
        assertEquals(ExecutionStatus.RUNNING_EXECUTION, p.getState());
        t.setTaskGroupId(0);
        t.setState(ExecutionStatus.SUCCESS);
        StateEvent event = new StateEvent();
        event.setProcessInstanceId(201);
        event.setTaskInstanceId(101);
        event.setType(StateEventType.TASK_STATE_CHANGE);
        event.setExecutionStatus(ExecutionStatus.SUCCESS);
        call(
            w,
            WorkflowExecuteThread.class,
            "taskStateChangeHandler",
            new Class<?>[] {StateEvent.class},
            event);
      } else {
        call(
            w,
            WorkflowExecuteThread.class,
            "submitPostNode",
            new Class<?>[] {String.class},
            (Object) null);
        verifyNoInteractions(netty);
      }
      assertEquals(ExecutionStatus.SUCCESS, p.getState());
      assertNotNull(p.getEndTime());
      assertEquals(101, t.getId());
      assertEquals(Flag.YES, t.getFlag());
    } finally {
      TaskProcessorFactory.PROCESS_MAP.clear();
      TaskProcessorFactory.PROCESS_MAP.putAll(saved);
      spring.setApplicationContext(previous);
    }
  }
}
