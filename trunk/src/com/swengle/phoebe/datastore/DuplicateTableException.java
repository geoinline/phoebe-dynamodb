/**
 * 
 */
package com.swengle.phoebe.datastore;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class DuplicateTableException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3820910901199076890L;

	/**
	 * 
	 */
	public DuplicateTableException() {
		// nothing to do
	}
	
	/**
	 * @param arg0
	 */
	public DuplicateTableException(String arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 */
	public DuplicateTableException(Throwable arg0) {
		super(arg0);
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public DuplicateTableException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
}
