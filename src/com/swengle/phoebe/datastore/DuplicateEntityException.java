/**
 * 
 */
package com.swengle.phoebe.datastore;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class DuplicateEntityException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6598790074919284029L;

	/**
	 * 
	 */
	public DuplicateEntityException() {
		// nothing to do
	}
	
	/**
	 * @param arg0
	 */
	public DuplicateEntityException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public DuplicateEntityException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public DuplicateEntityException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}
