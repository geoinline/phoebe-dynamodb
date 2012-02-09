/**
 * 
 */
package com.swengle.phoebe.query;

import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class RangeQueryEntityCounter<T> {
	private RangeQueryImpl<T> rangeQuery;
	private QueryRequest queryRequest;


	/**
	 * 
	 */
	public RangeQueryEntityCounter(RangeQueryImpl<T> rangeQuery) {
		this.rangeQuery = rangeQuery;
		queryRequest = rangeQuery.toQueryRequest();
		queryRequest.setCount(true);
	}
	
	public int getCount() {
		int count = 0;
		Key lastEvaluatedKey = queryRequest.getExclusiveStartKey();
		do {
			queryRequest.setExclusiveStartKey(lastEvaluatedKey);
		    QueryResult queryResult = rangeQuery.getPhoebe().getClient().query(
					queryRequest);
		    count += queryResult.getCount();
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
		} while (lastEvaluatedKey != null);
		return count;
	}

}
