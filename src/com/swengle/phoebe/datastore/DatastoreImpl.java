/**
 * 
 */
package com.swengle.phoebe.datastore;

import java.util.List;

import com.swengle.phoebe.Phoebe;
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
public class DatastoreImpl implements Datastore {
	private AsyncDatastore asyncDatastore;

	/**
	 * Create a new DatastoreImpl object
	 */
	public DatastoreImpl(Phoebe phoebe) {
		this(phoebe, false);
	}

	/**
	 * Create a new DatastoreImpl object
	 */
	public DatastoreImpl(Phoebe phoebe, boolean consistentRead) {
		if (consistentRead) {
			asyncDatastore = phoebe.getConsistentReadAsyncDatastore();
		} else {
			asyncDatastore = phoebe.getAsyncDatastore();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.Iterable)
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			Iterable<String> hashKeyRangeKeys) {
		return asyncDatastore.delete(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String)
	 */
	@Override
	public <T> Void delete(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String hashKeyRangeKey) {
		return asyncDatastore.delete(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String[])
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String... hashKeyRangeKeys) {
		return asyncDatastore.delete(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			Iterable<? extends Object> hashKeys) {
		return asyncDatastore.delete(kindClass, hashKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <T> Void delete(Class<T> kindClass, Object hashKey) {
		return asyncDatastore.delete(kindClass, hashKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(Class<T> kindClass,
			Object... hashKeys) {
		return asyncDatastore.delete(kindClass, hashKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Class, java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> Void delete(Class<T> kindClass, Object hashKey, Object rangeKey) {
		return asyncDatastore.delete(kindClass, hashKey, rangeKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(com.swengle.phoebe.key.EntityKey)
	 */
	@Override
	public <T> Void delete(EntityKey<T> entityKey) {
		return asyncDatastore.delete(entityKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Iterable)
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(
			Iterable<? extends EntityKey<? extends T>> entityKeys) {
		return asyncDatastore.delete(entityKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(com.swengle.phoebe.query.Query)
	 */
	@Override
	public <T> Iterable<DeleteResult<T>> delete(Query<T> query) {
		return asyncDatastore.delete(query).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#delete(java.lang.Object)
	 */
	@Override
	public <T> Void delete(T entity) {
		return asyncDatastore.delete(entity).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.Iterable)
	 */
	@Override
	public <T> List<T> get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			Iterable<String> hashKeyRangeKeys) {
		return asyncDatastore.get(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String)
	 */
	@Override
	public <T> T get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String hashKeyRangeKey) {
		return asyncDatastore.get(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, com.swengle.phoebe.key.HashKeyRangeKeyResolver, java.lang.String[])
	 */
	@Override
	public <T> List<T> get(Class<T> kindClass,
			HashKeyRangeKeyResolver hashKeyRangeKeyResolver,
			String... hashKeyRangeKeys) {
		return asyncDatastore.get(kindClass, hashKeyRangeKeyResolver, hashKeyRangeKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, java.lang.Iterable)
	 */
	@Override
	public <T> List<T> get(Class<T> kindClass,
			Iterable<? extends Object> hashKeys) {
		return asyncDatastore.get(kindClass, hashKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, java.lang.Object)
	 */
	@Override
	public <T> T get(Class<T> kindClass, Object hashKey) {
		return asyncDatastore.get(kindClass, hashKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, java.lang.Object[])
	 */
	@Override
	public <T> List<T> get(Class<T> kindClass, Object... hashKeys) {
		return asyncDatastore.get(kindClass, hashKeys).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Class, java.lang.Object, java.lang.Object)
	 */
	@Override
	public <T> T get(Class<T> kindClass, Object hashKey, Object rangeKey) {
		return asyncDatastore.get(kindClass, hashKey, rangeKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(com.swengle.phoebe.key.EntityKey)
	 */
	@Override
	public <T> T get(EntityKey<T> entityKey) {
		return asyncDatastore.get(entityKey).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#get(java.lang.Iterable)
	 */
	@Override
	public <T> List<T> get(Iterable<? extends EntityKey<? extends T>> entityKeys) {
		return asyncDatastore.get(entityKeys).now();
	}
	
	

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#insert(java.lang.Iterable)
	 */
	@Override
	public <T> Iterable<SaveResult<T>> insert(Iterable<? extends T> entities) {
		return asyncDatastore.insert(entities).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#insert(java.lang.Object)
	 */
	@Override
	public <T> Void insert(T entity) {
		return asyncDatastore.insert(entity).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#put(java.lang.Iterable)
	 */
	@Override
	public <T> Iterable<SaveResult<T>> put(Iterable<? extends T> entities) {
		return asyncDatastore.put(entities).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#put(java.lang.Object)
	 */
	@Override
	public <T> Void put(T entity) {
		return asyncDatastore.put(entity).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#update(com.swengle.phoebe.key.EntityKey, com.swengle.phoebe.query.UpdateOperations)
	 */
	@Override
	public <T> UpdateResult<T> update(EntityKey<T> entityKey,
			UpdateOperations<T> ops) {
		return asyncDatastore.update(entityKey, ops).now();
	}

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#update(java.lang.Iterable, com.swengle.phoebe.query.UpdateOperations)
	 */
	@Override
	public <T> Iterable<UpdateResult<T>> update(
			Iterable<EntityKey<T>> entityKeys, UpdateOperations<T> ops) {
		return asyncDatastore.update(entityKeys, ops).now();
	}


	/* (non-Javadoc)
	 * @see com.swengle.phoebe.datastore.Datastore#update(com.swengle.phoebe.query.Query, com.swengle.phoebe.query.UpdateOperations)
	 */
	@Override
	public <T> Iterable<UpdateResult<T>> update(Query<T> query,
			UpdateOperations<T> ops) {
		return asyncDatastore.update(query, ops).now();
	}

}
