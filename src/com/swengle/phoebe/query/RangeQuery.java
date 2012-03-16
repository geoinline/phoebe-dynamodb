package com.swengle.phoebe.query;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface RangeQuery<T> extends Query<T> {
	/**
	 * Limit the number of results returned
	 */
	RangeQuery<T> limit(int limit);
	
	/**
	 * The range condition to apply to the query
	 */
	RangeCondition<RangeQuery<T>> range();
	
	/**
	 * Exclusive start key from which to resume the query.
	 */
	ExclusiveStartKey<RangeQuery<T>> withExclusiveStartKey();
	
	/**
	 * Whether to scan the index forward (default true) or backward
	 */
	RangeQuery<T> directionForward(boolean directionForward);
}
