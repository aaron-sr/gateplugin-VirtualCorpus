package gate.virtualcorpus;

import java.io.Serializable;

import gate.persist.PersistenceException;
import gate.util.persistence.LRPersistence;

/**
 * {@link gate.util.persistence.CorpusPersistence} is not suitable since it
 * stores the complete docList
 * 
 * {@link VirtualCorpus} just need initParams, which are stored by
 * {@link gate.util.persistence.ResourcePersistence}
 */
public class VirtualCorpusPersistence extends LRPersistence {
	public static final long serialVersionUID = 2L;

	protected Serializable featureMap;

	@Override
	public void extractDataFromSource(Object source) throws PersistenceException {
		if (!(source instanceof VirtualCorpus)) {
			throw new UnsupportedOperationException(
					getClass().getName() + " can only be used for " + VirtualCorpus.class.getName() + " objects!\n"
							+ source.getClass().getName() + " is not a " + VirtualCorpus.class.getName());
		}

		super.extractDataFromSource(source);
	}

}
