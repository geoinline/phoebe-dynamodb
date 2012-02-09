/**
 * 
 */
package com.swengle.phoebe.query;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface ScanQuery<T> extends Query<T> {
	/**
	 * Optional filters to apply to fields (limiting the query).
	 */
	FieldCondition<ScanQuery<T>> field(String fieldName);
	/**
	 * Limit the number of results returned
	 */
	ScanQuery<T> limit(int limit);
	/**
	 * Exclusive start key from which to resume the query.
	 */
	ExclusiveStartKey<ScanQuery<T>> withExclusiveStartKey();
}
