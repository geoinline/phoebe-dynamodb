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
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.swengle.phoebe.reflect.DynamoDBReflector;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class ScanQueryEntityIterator<T> implements Iterator<T> {
	private static final Log LOG = LogFactory.getLog(ScanQueryEntityIterator.class);

	private ScanQueryImpl<T> scanQuery;
	private ScanRequest scanRequest;
	private Collection<Method> onReadMethods;
	private Key lastEvaluatedKey;
	private Iterator<Map<String, AttributeValue>> iterator;
	
	/**
	 * 
	 */
	public ScanQueryEntityIterator(ScanQueryImpl<T> scanQuery) {
		this.scanQuery = scanQuery;
		scanRequest = scanQuery.toScanRequest();
		onReadMethods = DynamoDBReflector.INSTANCE
				.getOnReadMethods(scanQuery.getKindClass());
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
		T entity = scanQuery.getPhoebe().marshallIntoObject(scanQuery.getKindClass(), iterator.next());
		for (Method onReadMethod : onReadMethods) {
			DynamoDBReflector.INSTANCE.safeInvoke(onReadMethod,
					entity);
		}
		return entity;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	private void loadBatch() {
		if (lastEvaluatedKey != null) {
			scanRequest.setExclusiveStartKey(lastEvaluatedKey);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("ScanRequest: " + scanRequest);
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
