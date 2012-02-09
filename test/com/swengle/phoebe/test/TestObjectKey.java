/**
 * 
 */
package com.swengle.phoebe.test;

import static org.junit.Assert.*;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBMappingException;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.key.InvalidEntityKeyException;
import com.swengle.phoebe.reflect.DynamoDBReflector;
import com.swengle.phoebe.test.model.ObjectWithHashKey;
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

/**
 * @author Administrator
 *
 */
public class TestObjectKey {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link com.swengle.phoebe.key.EntityKey#create(java.lang.Class, java.lang.Object)}.
	 */
	@Test
	public void testObjectKeyCreation() {
		try {
			EntityKey.create(TestObjectKey.class);
			fail("Expected failure when creating Key on non-DynamoDB model class");
		} catch (DynamoDBMappingException e) {
			// do nothing
		}

		try {
			EntityKey.create(new Object());
			fail("Expected failure when creating Key on non-DynamoDB model object");
		} catch (DynamoDBMappingException e) {
			// do nothing
		}
		
		ObjectWithHashKey objectWithHashKey = new ObjectWithHashKey();
		try {
			EntityKey.create(objectWithHashKey);
			fail("Expected failure when creating Key on model object with hashKey not set");
		} catch (InvalidEntityKeyException e) {
			// do nothing
		}
		
		String id = new ObjectId().toString();
		objectWithHashKey.setId(id);
		EntityKey<ObjectWithHashKey> key = EntityKey.create(objectWithHashKey);
		
		Assert.assertTrue(key.getKindClass().equals(ObjectWithHashKey.class));
		Assert.assertTrue(key.getHashKey().equals(objectWithHashKey.getId()));
		Assert.assertTrue(key.getRangeKey() == null);
		Assert.assertTrue(key.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKey.class).tableName()));
		
		key = EntityKey.create(ObjectWithHashKey.class, id);
		Assert.assertTrue(key.getKindClass().equals(ObjectWithHashKey.class));
		Assert.assertTrue(key.getHashKey().equals(id));
		Assert.assertTrue(key.getRangeKey() == null);
		Assert.assertTrue(key.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKey.class).tableName()));
		
		
		ObjectWithHashKeyAndRangeKey objectWithHashKeyAndRangeKey = new ObjectWithHashKeyAndRangeKey();
		try {
			EntityKey.create(objectWithHashKeyAndRangeKey);
			fail("Expected failure when creating Key on model object with unset hashKey and unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// do nothing
		}
		
		EntityKey<ObjectWithHashKeyAndRangeKey> key2;
		objectWithHashKeyAndRangeKey.setId(id);
		
		try {
			key2 = EntityKey.create(objectWithHashKeyAndRangeKey);
			fail("Expected failure creating Key on ObjectWithHashKeyAndRangeKey with unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// ignore
		}
			
		
		objectWithHashKeyAndRangeKey.setRangeId(id);
		key2 = EntityKey.create(objectWithHashKeyAndRangeKey);
		Assert.assertTrue(key2.getKindClass().equals(ObjectWithHashKeyAndRangeKey.class));
		Assert.assertTrue(key2.getHashKey().equals(objectWithHashKeyAndRangeKey.getId()));
		Assert.assertTrue(key2.getRangeKey().equals(id));
		Assert.assertTrue(key2.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKeyAndRangeKey.class).tableName()));
		
		try {
			key2 = EntityKey.create(ObjectWithHashKeyAndRangeKey.class, id, null);
			fail("Expected failure creating Key on ObjectWithHashKeyAndRangeKey class with unset rangeKey");
		} catch (InvalidEntityKeyException e) {
			// ignore
		}
		
		
		key2 = EntityKey.create(ObjectWithHashKeyAndRangeKey.class, id, id);
		Assert.assertTrue(key2.getKindClass().equals(ObjectWithHashKeyAndRangeKey.class));
		Assert.assertTrue(key2.getHashKey().equals(id));
		Assert.assertTrue(key2.getRangeKey().equals(id));
		Assert.assertTrue(key2.getDynamoDBTable().tableName().equals(DynamoDBReflector.INSTANCE.getTable(ObjectWithHashKeyAndRangeKey.class).tableName()));
		
		EntityKey<ObjectWithHashKeyAndRangeKey> keyTop = EntityKey.create(ObjectWithHashKeyAndRangeKey.class, id, id);
		EntityKey<ObjectWithHashKeyAndRangeKey> keyTopDitto = EntityKey.create(ObjectWithHashKeyAndRangeKey.class, id, id);
		EntityKey<ObjectWithHashKeyAndRangeKey> keyBottom = EntityKey.create(ObjectWithHashKeyAndRangeKey.class, id, new ObjectId().toString());

		Assert.assertTrue(keyTop.equals(keyTopDitto));
		Assert.assertTrue(keyTop.compareTo(keyTopDitto) == 0);
		Assert.assertTrue(keyTop.compareTo(keyBottom) == -1);
		Assert.assertTrue(keyBottom.compareTo(keyTop) == 1);
		Assert.assertTrue(keyTop.hashCode() == keyTopDitto.hashCode());

	}
}
