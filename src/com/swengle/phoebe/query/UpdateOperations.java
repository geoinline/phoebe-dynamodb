/**
 * 
 */
package com.swengle.phoebe.query;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public interface UpdateOperations<T> {
	/** adds the val to a set field **/
	public UpdateOperations<T> add(String fieldName, Object val);
	/** adds the vals to a set field **/
	public UpdateOperations<T> addAll(String fieldName, Iterable<?> vals);
	/** decrements the number field by 1 **/
	public UpdateOperations<T> dec(String fieldName);
	/** increments the number field by 1 **/
	public UpdateOperations<T> inc(String fieldName);
	/** increments the number field by value (negative values allowed) **/
	public UpdateOperations<T> inc(String fieldName, Number value);
	/** removes val from the set field **/
	public UpdateOperations<T> remove(String fieldName, Object val);
	/** removes vals from the set field **/
	public UpdateOperations<T> removeAll(String fieldName, Iterable<?> vals);
	/** sets the field to val **/
	public UpdateOperations<T> set(String fieldName, Object val);
	/** removes the field **/
	public UpdateOperations<T> unset(String fieldName);

}
