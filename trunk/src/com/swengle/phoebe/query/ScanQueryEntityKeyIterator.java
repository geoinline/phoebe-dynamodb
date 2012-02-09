/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.reflect.DynamoDBReflector;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ScanQueryEntityKeyIterator<T> implements Iterator<EntityKey<T>> {
	private ScanQueryImpl<T> scanQuery;
	private ScanRequest scanRequest;
	private Key lastEvaluatedKey;
	private Iterator<Map<String, AttributeValue>> iterator;
	
	/**
	 * 
	 */
	public ScanQueryEntityKeyIterator(ScanQueryImpl<T> scanQuery) {
		this.scanQuery = scanQuery;
		this.scanRequest = scanQuery.toScanRequest();
		String hashKeyAttributeName = DynamoDBReflector.INSTANCE.getHashKeyAttributeName(scanQuery.getKindClass());
		String rangeKeyAttributeName = DynamoDBReflector.INSTANCE.getRangeKeyAttributeName(scanQuery.getKindClass());
		if (rangeKeyAttributeName == null) {
			scanRequest.setAttributesToGet(Arrays.asList(hashKeyAttributeName));
		} else {
			scanRequest.setAttributesToGet(Arrays.asList(hashKeyAttributeName, rangeKeyAttributeName));
		}
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
	public EntityKey<T> next() {
		T entity = scanQuery.getPhoebe().marshallIntoObject(scanQuery.getKindClass(), iterator.next());
		return EntityKey.create(entity);
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
