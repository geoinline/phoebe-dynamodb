/**
 * 
 */
package com.swengle.phoebe.query;

import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;


/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ScanQueryEntityCounter<T> {
	private ScanQueryImpl<T> scanQuery;
	private ScanRequest scanRequest;

	/**
	 * 
	 */
	public ScanQueryEntityCounter(ScanQueryImpl<T> scanQuery) {
		this.scanQuery = scanQuery;
		scanRequest = scanQuery.toScanRequest();
		scanRequest.setCount(true);
	}
	
	public int getCount() {
		int count = 0;
		Key lastEvaluatedKey = scanRequest.getExclusiveStartKey();
		do {
			scanRequest.setExclusiveStartKey(lastEvaluatedKey);
		    ScanResult scanResult = scanQuery.getPhoebe().getClient().scan(
					scanRequest);
		    count += scanResult.getCount();
		    if (scanRequest.getLimit() != null) {
		    	int limitToFetch = scanRequest.getLimit() - scanResult.getCount();
		    	if (limitToFetch == 0) {
		    		lastEvaluatedKey = null;
		    	} else {
		    		scanRequest.setLimit(limitToFetch);
		    		lastEvaluatedKey = scanResult.getLastEvaluatedKey();
		    	}
		    } else {
		    	lastEvaluatedKey = scanResult.getLastEvaluatedKey();
		    }
		} while (lastEvaluatedKey != null);
		return count;
	}

}
