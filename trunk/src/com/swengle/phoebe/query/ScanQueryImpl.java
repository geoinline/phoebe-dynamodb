/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.Condition;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ScanQueryImpl<T> extends QueryImpl<T> implements ScanQuery<T> {
	private int limit;
	private List<FieldConditionImpl<ScanQuery<T>>> fieldConditionList;
	private ExclusiveStartKeyImpl<ScanQuery<T>> exclusiveStartKey;
	
	/**
	 * Create a new ScanQueryImpl object
	 */
	public ScanQueryImpl(Phoebe phoebe, Class<T> kindClass) {
		super(phoebe, kindClass);
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#asList()
	 */
	@Override
	public List<T> asList() {
		List<T> result = new ArrayList<T>();
		Iterator<T> entityIterator = fetch();
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
		List<EntityKey<T>> result = new ArrayList<EntityKey<T>>();
		Iterator<EntityKey<T>> entityKeyIterator = fetchEntityKeys();
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
		ScanQueryEntityCounter<T> scanQueryEntityCounter = new ScanQueryEntityCounter<T>(this);
		return scanQueryEntityCounter.getCount();
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#fetch()
	 */
	@Override
	public Iterator<T> fetch() {
		return new ScanQueryEntityIterator<T>(this);
	}

	/* (non-Javadoc)s
	 * @see com.phoebe.dynamodb.datastore.query.QueryResults#fetchEntityKeys()
	 */
	@Override
	public Iterator<EntityKey<T>> fetchEntityKeys() {
		return new ScanQueryEntityKeyIterator<T>(this);
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
	 * @see com.phoebe.dynamodb.datastore.query.ScanQuery2#field(java.lang.String)
	 */
	@Override
	public FieldCondition<ScanQuery<T>> field(String fieldName) {
		FieldConditionImpl<ScanQuery<T>> field = new FieldConditionImpl<ScanQuery<T>>(this, fieldName);
		if (fieldConditionList == null) {
			fieldConditionList = new ArrayList<FieldConditionImpl<ScanQuery<T>>>();
		}
		fieldConditionList.add(field);
		return field;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.ScanQuery2#limit(int)
	 */
	@Override
	public ScanQuery<T> limit(int limit) {
		this.limit = limit;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.ScanQuery2#withExclusiveStartKey()
	 */
	@Override
	public ExclusiveStartKey<ScanQuery<T>> withExclusiveStartKey() {
		exclusiveStartKey = new ExclusiveStartKeyImpl<ScanQuery<T>>(this);
		return exclusiveStartKey;
	}
	
	/**
	 * Generate the ScanRequest based on the currently constructed query
	 */
	public ScanRequest toScanRequest() {
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
		if (limit > 0) {
			scanRequest.withLimit(limit);
		}
		if (attributesToGet != null) {
			scanRequest.withAttributesToGet(attributesToGet);
		}
		if (exclusiveStartKey != null) {
			scanRequest.withExclusiveStartKey(exclusiveStartKey.getEntityKey().getDynamoDBKey());
		}
		if (fieldConditionList != null && fieldConditionList.size() > 0) {
			Map<String, Condition> scanFilter = new HashMap<String, Condition>();
			for (FieldConditionImpl<ScanQuery<T>> field: fieldConditionList) {
				scanFilter.put(field.getAttributeName(), field.getCondition());
			}
			scanRequest.withScanFilter(scanFilter);
		}

		return scanRequest;
	}


}

