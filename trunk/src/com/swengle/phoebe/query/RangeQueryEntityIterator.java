/**
 * 
 */
package com.swengle.phoebe.query;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.swengle.phoebe.reflect.DynamoDBReflector;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class RangeQueryEntityIterator<T> implements Iterator<T> {
	private static final Log LOG = LogFactory.getLog(RangeQueryEntityIterator.class);

	
	private RangeQueryImpl<T> rangeQuery;
	private QueryRequest queryRequest;
	private Collection<Method> onReadMethods;
	private Key lastEvaluatedKey;
	private Iterator<Map<String, AttributeValue>> iterator;
	
	/**
	 * 
	 */
	public RangeQueryEntityIterator(RangeQueryImpl<T> rangeQuery) {
		this.rangeQuery = rangeQuery;
		queryRequest = rangeQuery.toQueryRequest();
		onReadMethods = DynamoDBReflector.INSTANCE
				.getOnReadMethods(rangeQuery.getKindClass());
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
	public T next() {
		T entity = rangeQuery.getPhoebe().marshallIntoObject(rangeQuery.getKindClass(), iterator.next());
		for (Method onReadMethod : onReadMethods) {
			DynamoDBReflector.INSTANCE.safeInvoke(onReadMethod,
					entity);
		}
		return entity;
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
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("ScanRequest: " + queryRequest);
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
