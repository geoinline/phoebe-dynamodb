/**
 * 
 */
package com.swengle.phoebe.key;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class DelimiterHashKeyRangeKeyResolver implements HashKeyRangeKeyResolver {
	private String delimiter;

	/**
	 * Create a new DelimiterHashKeyRangeKeyResolver object
	 */
	public DelimiterHashKeyRangeKeyResolver(String delimiter) {
		this.delimiter = delimiter;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.key.HashKeyRangeKeyResolver#split(java.lang.String)
	 */
	@Override
	public String[] split(String hashKeyRangeKey) {
		return hashKeyRangeKey.split(delimiter);
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.key.HashKeyRangeKeyResolver#join(java.lang.Object, java.lang.Object)
	 */
	@Override
	public String join(Object hashKey, Object rangeKey) {
		return String.valueOf(hashKey) + delimiter + String.valueOf(rangeKey);
	}

}
