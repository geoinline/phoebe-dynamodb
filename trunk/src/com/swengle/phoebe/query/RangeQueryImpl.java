/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.reflect.DynamoDBReflector.MarshallerType;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class RangeQueryImpl<T> extends QueryImpl<T> implements RangeQuery<T>  {
	private int limit;
	private boolean directionForward = true;
	private ExclusiveStartKeyImpl<RangeQuery<T>> exclusiveStartKey;
	private RangeConditionImpl<RangeQuery<T>> rangeCondition;
	private AttributeValue hashKeyAttributeValue;
	private boolean consistentRead;
	
	/**
	 * Create a new RangeQueryImpl object
	 */
	public RangeQueryImpl(Phoebe phoebe, Class<T> kindClass, Object hashKey) {
		this(phoebe, kindClass, hashKey, false);
	}
	
	/**
	 * Create a new RangeQueryImpl object
	 */
	public RangeQueryImpl(Phoebe phoebe, Class<T> kindClass, Object hashKey, boolean consistentRead) {
		super(phoebe, kindClass);
		if (DynamoDBReflector.INSTANCE.getRangeKeyGetter(kindClass) == null) {
			throw new InvalidQueryException("rangeKey is not defined for "
					+ kindClass);
		}
		MarshallerType marshallerType = DynamoDBReflector.INSTANCE.getMarshallerType(DynamoDBReflector.INSTANCE.getHashKeyGetter(kindClass));
		if (marshallerType == MarshallerType.S) {
			hashKeyAttributeValue = new AttributeValue().withS(String.valueOf(hashKey));
		} else {
			hashKeyAttributeValue = new AttributeValue().withN(String.valueOf(hashKey));
		}
		this.consistentRead = consistentRead;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#asList()
	 */
	@Override
	public List<T> asList() {
		Iterator<T> entityIterator = fetch();
		List<T> result = new ArrayList<T>();
		while (entityIterator.hasNext()) {
			result.add(entityIterator.next());
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#asEntityKeyList()
	 */
	@Override
	public List<EntityKey<T>> asEntityKeyList() {
		Iterator<EntityKey<T>> entityKeyIterator = fetchEntityKeys();
		List<EntityKey<T>> result = new ArrayList<EntityKey<T>>();
		while (entityKeyIterator.hasNext()) {
			result.add(entityKeyIterator.next());
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#count()
	 */
	@Override
	public int count() {
		RangeQueryEntityCounter<T> rangeQueryEntityCounter = new RangeQueryEntityCounter<T>(this);
		return rangeQueryEntityCounter.getCount();
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#fetch()
	 */
	@Override
	public Iterator<T> fetch() {
		return new RangeQueryEntityIterator<T>(this);
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#fetchEntityKeys()
	 */
	@Override
	public Iterator<EntityKey<T>> fetchEntityKeys() {
		return new RangeQueryEntityKeyIterator<T>(this);
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#get()
	 */
	@Override
	public T get() {
		int origLimit = limit;
		limit = 1;
		List<T> objList = this.asList();
		limit = origLimit;
		if (objList.size() != 1) {
			return null;
		} else {
			return objList.get(0);
		}
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#getEntityKey()
	 */
	@Override
	public EntityKey<T> getEntityKey() {
		int origLimit = limit;
		limit = 1;
		List<EntityKey<T>> objKeyList = this.asEntityKeyList();
		limit = origLimit;
		if (objKeyList.size() != 1) {
			return null;
		} else {
			return objKeyList.get(0);
		}
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeQuery2#limit(int)
	 */
	@Override
	public RangeQuery<T> limit(int limit) {
		this.limit = limit;
		return this;
	}
	
	

	/* (non-Javadoc)
	 * @see com.swengle.phoebe.query.RangeQuery#directionForward(boolean)
	 */
	@Override
	public RangeQuery<T> directionForward(boolean directionForward) {
		this.directionForward = directionForward;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeQuery2#range()
	 */
	@Override
	public RangeCondition<RangeQuery<T>> range() {
		if (rangeCondition != null) {
			throw new InvalidQueryException("rangeCondition can only be called once");
		}
		rangeCondition = new RangeConditionImpl<RangeQuery<T>>(this);
		return rangeCondition;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.RangeQuery2#withExclusiveStartKey()
	 */
	@Override
	public ExclusiveStartKey<RangeQuery<T>> withExclusiveStartKey() {
		exclusiveStartKey = new ExclusiveStartKeyImpl<RangeQuery<T>>(this);
		return exclusiveStartKey;
	}
	
	/**
	 * Generate the QueryRequest based on the currently constructed query
	 */
	public QueryRequest toQueryRequest() {
		QueryRequest queryRequest = new QueryRequest().withTableName(tableName).withHashKeyValue(hashKeyAttributeValue);
		queryRequest.withConsistentRead(consistentRead);
		queryRequest.withScanIndexForward(directionForward);
		if (limit > 0) {
			queryRequest.withLimit(limit);
		}
		if (attributesToGet != null) {
			queryRequest.withAttributesToGet(attributesToGet);
		}
		if (exclusiveStartKey != null) {
			queryRequest.withExclusiveStartKey(exclusiveStartKey.getEntityKey().getDynamoDBKey());
		}
		if (rangeCondition != null) {
			queryRequest.withRangeKeyCondition(rangeCondition.getCondition());
		}

		return queryRequest;
	}	

}
