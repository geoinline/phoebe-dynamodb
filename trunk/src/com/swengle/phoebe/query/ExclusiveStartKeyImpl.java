/**
 * 
 */
package com.swengle.phoebe.query;

import com.swengle.phoebe.key.EntityKey;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ExclusiveStartKeyImpl<T extends Query<?>> implements ExclusiveStartKey<T> {
	private T query;
	private EntityKey<?> entityKey;

	/**
	 * Create a new ExclusiveStartKeyImpl object
	 */
	public ExclusiveStartKeyImpl(T query) {
		this.query = query;
	}

	/**
	 * @return the entityKey
	 */
	public EntityKey<?> getEntityKey() {
		return entityKey;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.ExclusiveStartKey#equal(java.lang.Object)
	 */
	@Override
	public T equal(Object hashKey) {
		this.entityKey = EntityKey.create(this.query.getKindClass(), hashKey);
		return this.query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.ExclusiveStartKey#equal(java.lang.Object, java.lang.Object)
	 */
	@Override
	public T equal(Object hashKey, Object rangeKey) {
		this.entityKey = EntityKey.create(this.query.getKindClass(), hashKey, rangeKey);
		return this.query;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.ExclusiveStartKey#equal(com.phoebe.dynamodb.datastore.key.EntityKey)
	 */
	@Override
	public T equal(EntityKey<T> entityKey) {
		this.entityKey = entityKey;
		return this.query;
	}
	
	

}
