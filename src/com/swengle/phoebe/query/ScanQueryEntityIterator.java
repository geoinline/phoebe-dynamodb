/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ScanQueryEntityIterator<T> implements Iterator<T> {
	private ScanQueryImpl<T> scanQuery;
	private ScanRequest scanRequest;
	private Key lastEvaluatedKey;
	private Iterator<Map<String, AttributeValue>> iterator;
	
	/**
	 * 
	 */
	public ScanQueryEntityIterator(ScanQueryImpl<T> scanQuery) {
		this.scanQuery = scanQuery;
		scanRequest = scanQuery.toScanRequest();
	}

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

	@Override
	public T next() {
		return scanQuery.getPhoebe().marshallIntoObject(scanQuery.getKindClass(), iterator.next());
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	private void loadBatch() {
		if (lastEvaluatedKey != null) {
			scanRequest.setExclusiveStartKey(lastEvaluatedKey);
		}
		ScanResult scanResult = scanQuery.getPhoebe().getClient().scan(scanRequest);
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
		iterator = scanResult.getItems().iterator();
	}

}
