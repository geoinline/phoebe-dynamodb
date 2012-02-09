/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.reflect.DynamoDBReflector;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class RangeQueryEntityKeyIterator<T> implements Iterator<EntityKey<T>> {
	private RangeQueryImpl<T> rangeQuery;
	private QueryRequest queryRequest;
	private Key lastEvaluatedKey;
	private Iterator<Map<String, AttributeValue>> iterator;
	
	/**
	 * 
	 */
	public RangeQueryEntityKeyIterator(RangeQueryImpl<T> rangeQuery) {
		this.rangeQuery = rangeQuery;
		this.queryRequest = rangeQuery.toQueryRequest();
		String hashKeyAttributeName = DynamoDBReflector.INSTANCE.getHashKeyAttributeName(rangeQuery.getKindClass());
		String rangeKeyAttributeName = DynamoDBReflector.INSTANCE.getRangeKeyAttributeName(rangeQuery.getKindClass());
		if (rangeKeyAttributeName == null) {
			queryRequest.setAttributesToGet(Arrays.asList(hashKeyAttributeName));
		} else {
			queryRequest.setAttributesToGet(Arrays.asList(hashKeyAttributeName, rangeKeyAttributeName));
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if (iterator == null) {
			loadBatch();
		}
		if (iterator.hasNext()) {
			return true;
		} else if (lastEvaluatedKey != null) {
			loadBatch();
		}
		return iterator.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public EntityKey<T> next() {
		T entity = rangeQuery.getPhoebe().marshallIntoObject(rangeQuery.getKindClass(), iterator.next());
		return EntityKey.create(entity);
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	private void loadBatch() {
		if (lastEvaluatedKey != null) {
			queryRequest.setExclusiveStartKey(lastEvaluatedKey);
		}
		QueryResult queryResult = rangeQuery.getPhoebe().getClient().query(queryRequest);
		if (queryRequest.getLimit() != null) {
	    	int limitToFetch = queryRequest.getLimit() - queryResult.getCount();
	    	if (limitToFetch == 0) {
	    		lastEvaluatedKey = null;
	    	} else {
	    		queryRequest.setLimit(limitToFetch);
	    		lastEvaluatedKey = queryResult.getLastEvaluatedKey();
	    	}
	    } else {
	    	lastEvaluatedKey = queryResult.getLastEvaluatedKey();
	    }
		iterator = queryResult.getItems().iterator();
	}

}
