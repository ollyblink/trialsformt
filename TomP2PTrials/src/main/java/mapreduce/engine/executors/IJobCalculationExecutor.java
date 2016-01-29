package mapreduce.engine.executors;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;

public interface IJobCalculationExecutor {

	public void switchDataFromTaskToProcedureDomain(Procedure procedure, Task task);

	public CompletedBCMessage tryCompletingProcedure(Procedure procedure);

	public void executeTask(Task task, Procedure procedure);

	public PerformanceInfo performanceInformation();
 

}
