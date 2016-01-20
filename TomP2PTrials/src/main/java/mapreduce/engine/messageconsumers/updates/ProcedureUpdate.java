package mapreduce.engine.messageconsumers.updates;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate extends AbstractUpdate {
	// private static Logger logger = LoggerFactory.getLogger(ProcedureUpdate.class);

	private Job job;
	private JobCalculationMessageConsumer msgConsumer;

	public ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer) {
		this.job = job;
		this.msgConsumer = msgConsumer;
	}

	@Override
	protected void internalUpdate(IDomain outputDomain, Procedure procedure) {
		JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;
		procedure.addOutputDomain(outputJPD);
		if (procedure.isFinished()) {
			msgConsumer.cancelProcedureExecution(procedure.dataInputDomain().toString());
			JobProcedureDomain resultOutputDomain = procedure.resultOutputDomain();
			job.incrementProcedureIndex();
			job.currentProcedure().dataInputDomain(resultOutputDomain);
		}
	}
}
// Below code should not be needed as EndProcedure is always finished...
// if
// (job.currentProcedure().executable().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName()))
// {
// job.isFinished(true);
// }