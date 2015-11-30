package mapreduce.execution.broadcasthandler.messageconsumer.jobstatusmanager;

public class FinishedAllTasksJobStatusManager extends AbstractJobStatusManager {

	public static FinishedAllTasksJobStatusManager newInstance() {
		return new FinishedAllTasksJobStatusManager();
	}

	private FinishedAllTasksJobStatusManager() {

	}

	@Override
	protected void manageMessages() {
		// TODO Auto-generated method stub
		
	}

}