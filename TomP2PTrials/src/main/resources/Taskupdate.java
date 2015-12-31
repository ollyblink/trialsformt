import mapreduce.execution.computation.ProcedureInformation;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Tasks;

public class Taskupdate {
	// Make sure the transmitted job only contains procedure information for the tasks that need to be updated...
	// Don't forget the isFinished for all Job/Procedure/Task and isActive Job/Task
	public void TaskUpdate(Job receivedJob, Task receivedTask, TaskResult taskResultUpdate) {
		//Checking the jobs
		// Job here needs to only have tasks for the currently executed procedure used
		if (jobs.contains(receivedJob)) { // If it contains it, we may update it
			synchronized (jobs) {
				Job jobToUpdate = jobs.get(jobs.indexOf(receivedJob));
			}
			if (jobToUpdate.isActive()) { // We only want to update a currently executed job
				// Check the current procedure's actuality...
				ProcedureInformation currentPI = jobToUpdate.currentProcedure();
				ProcedureInformation receivedPI = receivedJob.currentProcedure();
				//Checking the procedures' orders: are the same procedures executed?
				if (currentPI.procedureIndex() == receivedPI.procedureIndex()) { 
					// This is what we hope for... the received index is the same as the currently executed job... Everything fine and can start comparing the tasks...
					List<Task> tasks = currentPI.tasks(); //Be aware, these task list is also used by the taskScheduler in jobExecutor!
					//Checking the tasks... Either there is a new task received that we were not yet executing, or an already executed task has to be updated
					synchronized (tasks) {
						if (tasks.contains(receivedTask)) { // The task was already once created or received... How cool 
							Tasks.updateStati(receivedTask, taskResultUpdate, receivedJob.maxNrOfFinishedWorkersPerTask());
						} else { // This executor has not yet had that task which means it has not created it yet... simply add id to the tasks
									// and it
									// can
									// be scheduled for execution
							tasks.add(receivedTask);
							// Task scheduler takes care of scheduling the task in case the execution stopped and the time to live did not yet run
							// out...
							// This is a treaky one... If we use a TimeToLive to decide if the Procedure is finished
							// Hmn...
							// Why not use a little average time for that? Always add received message time and use the max waiting time (Plus
							// some
							// delta)
							// to wait in the scheduler...
						}
					}
				} else /*if(currentPI.procedureIndex() != receivedPI.procedureIndex())*/{ // Abnormal case, something wrong because the executed procedure is not the same...
					if (currentPI.procedureIndex() < receivedPI.procedureIndex()) { // The current job is outdated
						// BTW currentProcedureIndex is needed as we don't know how FAR AWAY it is (e.g. current1 = 0, current2 = 2.. So don't
						// simply use previousProcedure() 
						//Let's hope at least the procedureIndex is in a reasonable range (>=0 and smaller than number of procedures...
						List<Task> oldTasksOfReceivedJob = receivedJob.procedure(currentPI.procedureIndex()).tasks(); // Or get it over DHT (getAll(PROCEDURE_KEYS)) if we clear that data after the procedure finished...
						synchronized (oldTasksOfReceivedJob) {
							if (!oldTasksOfReceivedJob.contains(receivedTask)) {
								//Well that's good for THIS executor... It executes correctly, meaning that it got a task that was not yet executed... All the others need to rewind as soon as they notice...
							} else {
								// We are outdated!! Need to update to received job 
								handleReceivedJob(receivedJob)
							}
						}
					} else if (currentPI.procedureIndex() > receivedPI.procedureIndex()) { // received message that is presumably outdated
						//This executor executes a procedure that is further in the processing chain than the received one
						// Hmn, may be because either the message received is out of date, or there was a task assigned that was not yet assigned
						// before the procedure in this executor ended...
						List<Task> oldTasksOfJob = receivedJob.procedure(currentPI.procedureIndex()).tasks(); // Or get it over DHT (getAll(PROCEDURE_KEYS)) if we clear that data after the procedure finished...
						synchronized (oldTasksOfJob) {
							if (!oldTasksOfJob.contains(receivedTask)) {
								/* 
								 * This is crap. That means the received task is from an older procedure and was not yet executed. So the  currently 
								 * executed procedure needs to be stopped, data removed, and the jobs current procedure index needs to be set back
								 * to the received one... But even better... I don't care here and simply remove it from the list and reschedule...
								 */
								jobs.remove(jobToUpdate);
								jobExecutor.broadcastRemoveData(job); // Let Michigan handle it (... meaning the DHT cleaner process ;))
								handleReceivedJob(receivedJob);
							} else {
								// Nothing to worry, this is just an old message and thus can be discarded...
							}
						}
					} else /*if(currentPI.procedureIndex() == receivedPI.procedureIndex())*/{
						// Not possible as this case is covered by the previous if else statement (if(currentlyExecutedJob.procedureIndex() ==
						// job.procedureIndex()))
					}
				}
			} else /* if (!jobToUpdate.isActive()) */ {
				// Well another job is executing... so simply treat it as new job received and let Michigan handle it
				handleReceivedJob(job);
			}
		} else /* if(!jobs.contains(job)) */ {
			// Apparently we have not received this job yet (in case a new executor comes online)...
			// Treat it as if a new job was received... Which is the same as if the next procedure is executed
			handleReceivedJob(job);
		}
	}
}
