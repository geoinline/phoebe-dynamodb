/**
 * 
 */
package com.swengle.phoebe.result;

import com.swengle.phoebe.key.EntityKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class UpdateResult<T> {
	private EntityKey<T> entityKey;
	private Exception exception;
	private boolean updated;

	/**
	 * Create a new UpdateResult object
	 */
	public UpdateResult(EntityKey<T> entityKey) {
		this.entityKey = entityKey;
	}

	/**
	 * @return the exception
	 */
	public Exception getException() {
		return exception;
	}

	/**
	 * @param exception the exception to set
	 */
	public void setException(Exception exception) {
		this.exception = exception;
	}

	/**
	 * @return the updated
	 */
	public boolean isUpdated() {
		return updated;
	}

	/**
	 * @param updated the updated to set
	 */
	public void setUpdated(boolean updated) {
		this.updated = updated;
	}


	/**
	 * @return the entityKey
	 */
	public EntityKey<T> getEntityKey() {
		return entityKey;
	}

	/**
	 * @param entityKey the entityKey to set
	 */
	public void setEntityKey(EntityKey<T> entityKey) {
		this.entityKey = entityKey;
	}
	
	/**
	 * Whether an exception occurred
	 */
	public boolean hasException() {
		return exception != null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{entityKey: " + entityKey + ", ");
		sb.append("exception: " + exception + ", ");
		sb.append("updated: " + updated + "}");
		return sb.toString();
	}

}
