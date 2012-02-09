/**
 * 
 */
package com.swengle.phoebe.query;

import com.swengle.phoebe.key.EntityKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface ExclusiveStartKey<T>  {
	/**
	 * 
	 * Equal to a hash key
	 */
	T equal(Object hashKey);
	
	/**
	 * 
	 * Equal to a composite hash/range key
	 */
	T equal(Object hashKey, Object rangeKey);
	
	/**
	 * 
	 * Equal to an Entity Key
	 */
	T equal(EntityKey<T> entityKey);
}
