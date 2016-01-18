package mapreduce.engine.messageconsumers.updates;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;

public class TaskUpdate extends AbstractUpdate {
	private static Logger logger = LoggerFactory.getLogger(TaskUpdate.class);

	private JobCalculationMessageConsumer msgConsumer;

	public TaskUpdate(JobCalculationMessageConsumer msgConsumer) {
		this.msgConsumer = msgConsumer;
	}

	@Override
	protected Procedure internalUpdate(IDomain outputDomain, Procedure procedure) {
		try {
			ExecutorTaskDomain outputETDomain = (ExecutorTaskDomain) outputDomain;
			Task receivedTask = Task.create(outputETDomain.taskId());
			List<Task> tasks = procedure.tasks();
			Task task = receivedTask;
			if (!tasks.contains(task)) {
				procedure.addTask(task);
			} else {
				task = tasks.get(tasks.indexOf(task));
			}
			if (!task.isFinished()) {// Is finished before adding new output procedure domain? then ignore update
				task.addOutputDomain(outputETDomain);
				// Is finished anyways or after adding new output procedure domain? then abort any executions of this task and
				if (task.isFinished()) {
					// transfer the task's output <K,{V}> to the procedure domain
					msgConsumer.cancelTaskExecution(procedure, task); // If so, no execution needed anymore
					// Transfer data to procedure domain! This may cause the procedure to become finished
					msgConsumer.executor().switchDataFromTaskToProcedureDomain(procedure, task);
				}
			}
			return procedure;
		} catch (Exception e) {
			logger.warn("Exception caught", e);
			return procedure;
		}
	}
}
