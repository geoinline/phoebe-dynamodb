/**
 * 
 */
package com.swengle.phoebe.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.reflect.DynamoDBReflector;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public abstract class QueryImpl<T> implements Query<T> {
	private Phoebe phoebe;
	protected Class<T> kindClass;
	protected String tableName;
	protected Collection<String> attributesToGet;
	
	/**
	 * Create a new QueryImpl object
	 * @param phoebe
	 * @param kindClass
	 */
	public QueryImpl(Phoebe phoebe, Class<T> kindClass) {
		this.phoebe = phoebe;
		this.kindClass = kindClass;
		this.tableName = DynamoDBReflector.INSTANCE.getTable(kindClass)
				.tableName();
	}
	
	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.Query#retrievedFields(java.util.Collection)
	 */
	@Override
	public final Query<T> retrievedFields(Collection<String> fields) {
		String attributeName;
		attributesToGet = new ArrayList<String>();
		for (String field: fields) {
			attributeName = DynamoDBReflector.INSTANCE.fieldToAttributeName(kindClass, field);
			if (attributeName == null) {
				throw new InvalidQueryException("Unknown field: " + field);
			}
			attributesToGet.add(attributeName);
		}
		return this;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.Query#retrievedFields(java.lang.String[])
	 */
	@Override
	public final Query<T> retrievedFields(String... fields) {
		retrievedFields(Arrays.asList(fields));
		return this;
	}

	/* (non-Javadoc)
	 * @see com.phoebe.dynamodb.datastore.query.Query#getKindClass()
	 */
	@Override
	public final Class<T> getKindClass() {
		return kindClass;
	}

	/**
	 * @return the phoebe
	 */
	public Phoebe getPhoebe() {
		return phoebe;
	}


}
