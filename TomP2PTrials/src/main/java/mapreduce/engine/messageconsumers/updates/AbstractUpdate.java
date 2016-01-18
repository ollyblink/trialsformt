package mapreduce.engine.messageconsumers.updates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.domains.IDomain;
import mapreduce.execution.procedures.Procedure;

public abstract class AbstractUpdate implements IUpdate {
	private static Logger logger = LoggerFactory.getLogger(AbstractUpdate.class);

	@Override
	public Procedure executeUpdate(IDomain outputDomain, Procedure procedure) {
		logger.info(outputDomain + " " + procedure);
		if (outputDomain != null && procedure != null) {
			try {
				return internalUpdate(outputDomain, procedure);
			} catch (Exception e) {
				logger.warn("Exception caught", e);
				return procedure;
			}
		} else {
			logger.warn("No update, either output domain or procedure or both were null.");
		}
		return procedure;
	}

	/**
	 * Template method, as the executeUpdate takes care of nullpointers
	 * 
	 * @param outputDomain
	 * @param procedure
	 */
	protected abstract Procedure internalUpdate(IDomain outputDomain, Procedure procedure) throws ClassCastException, NullPointerException;

	protected AbstractUpdate() {

	}
}