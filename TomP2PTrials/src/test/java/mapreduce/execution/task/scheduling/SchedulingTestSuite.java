package mapreduce.execution.task.scheduling;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskSchedulerTest;
import mapreduce.execution.task.scheduling.taskresultcomparisonscheduling.MinAssignedWorkersTaskResultComparisonSchedulerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	MinAssignedWorkersTaskSchedulerTest.class, 
	MinAssignedWorkersTaskResultComparisonSchedulerTest.class })
public class SchedulingTestSuite {

}
