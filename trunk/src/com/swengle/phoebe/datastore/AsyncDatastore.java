/**
 * 
 */
package com.swengle.phoebe.datastore;

import java.util.List;

import com.swengle.phoebe.concurrent.FutureResult;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.key.HashKeyRangeKeyResolver;
import com.swengle.phoebe.query.Query;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.result.DeleteResult;
import com.swengle.phoebe.result.SaveResult;
import com.swengle.phoebe.result.UpdateResult;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 * 
 */
public interface AsyncDatastore {
	/** Creates the table for the given class, if @DynamoDBTableInitialCapacities is defined that is used instead of the passed units **/
	<T> FutureResult<Void> createTable(Class<T> kindClass, long readCapacityUnits, long writeCapacityUnits) throws DuplicateTableException;
	/** Deletes the given entities by hashKey and rangeKey **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, Iterable<String> hashKeyRangeKeys);
	/** Deletes the given entity by hashKey and rangeKey **/
	<T> FutureResult<Void> delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String hashKeyRangeKey);
	/** Deletes the given entities by hashKey and rangeKey **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String... hashKeyRangeKeys);
	/** Deletes the given entities by hashKey **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(Class<T> kindClass,
			Iterable<? extends Object> hashKeys);
	/** Deletes the given entity by hashKey **/
	<T> FutureResult<Void> delete(Class<T> kindClass, Object hashKey);
	/** Deletes the given entities by hashKey **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(Class<T> kindClass,
			Object... hashKeys);
	/** Deletes the given entity by hashKey and rangeKey **/
	<T> FutureResult<Void> delete(Class<T> kindClass, Object hashKey,
			Object rangeKey);
	/** Deletes the given entity by EntityKey **/
	<T> FutureResult<Void> delete(EntityKey<T> entityKey);
	/** Deletes the given entities by EntityKey **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Iterable<? extends EntityKey<? extends T>> entityKeys);
	/** Deletes the given entities based on the query **/
	<T> FutureResult<Iterable<DeleteResult<T>>> delete(
			Query<T> query);
	
	
	
	/** Deletes the given entity (by EntityKey) **/
	<T> FutureResult<Void> delete(T entity);
	/** Drops the table for the given class **/
	<T> FutureResult<Void> dropTable(Class<T> kindClass);
	/** find the given entities by hashKey and rangeKey **/
	<T> FutureResult<List<T>> get(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, Iterable<String> hashKeyRangeKeys);
	/** find the given entity by hashKey and rangeKey **/
	<T> FutureResult<T> get(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String hashKeyRangeKey);
	/** find the given entities by hashKey and rangeKey **/
	<T> FutureResult<List<T>> get(Class<T> kindClass, HashKeyRangeKeyResolver hashKeyRangeKeyResolver, String... hashKeyRangeKeys);
	/** find the given entities by hashKey **/
	<T> FutureResult<List<T>> get(Class<T> kindClass,
			Iterable<? extends Object> hashKeys);
	/** find the given entity by hashKey **/
	<T> FutureResult<T> get(Class<T> kindClass, Object hashKey);
	/** find the given entities by hashKeys **/
	<T> FutureResult<List<T>> get(Class<T> kindClass, Object... hashKeys);
	/** find the given entity by hashKey and rangeKey **/
	<T> FutureResult<T> get(Class<T> kindClass, Object hashKey,
			Object rangeKey);
	/** find the given entity by EntityKey **/
	<T> FutureResult<T> get(EntityKey<T> entityKey);
	
	/** find the given entities by EntityKey **/
	<T> FutureResult<List<T>> get(
			Iterable<? extends EntityKey<? extends T>> entityKeys);
	/** find the given entities for a query **/
	<T> FutureResult<List<T>> get(Query<T> query);
	
	/** Save the given entities only if they do not already exist **/
	<T> FutureResult<Iterable<SaveResult<T>>> insert(
			Iterable<? extends T> entities);
	/**Save the given entity only if it does not already exist **/
	<T> FutureResult<Void> insert(T entity);
	
	
	/** Save the given entities (will update if entity already exists) **/
	<T> FutureResult<Iterable<SaveResult<T>>> put(
			Iterable<? extends T> entities);
	/** Save the given entity (will update if entity already exists) **/
	<T> FutureResult<Void> put(T entity);
	/** Updates the given entity by EntityKey **/
	<T> FutureResult<UpdateResult<T>> update(EntityKey<T> entityKey, UpdateOperations<T> ops);
	
	/** Updates the given entities by EntityKey **/
	<T> FutureResult<Iterable<UpdateResult<T>>> update(
			Iterable<EntityKey<T>> entityKeys, UpdateOperations<T> ops);
	/** Updates the given entities based on the query **/
	<T> FutureResult<Iterable<UpdateResult<T>>> update(
			Query<T> query, UpdateOperations<T> ops);
}
