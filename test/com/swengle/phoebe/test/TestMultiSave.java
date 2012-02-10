/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.result.SaveResult;
import com.swengle.phoebe.test.model.EntityWithHashKey;
import com.swengle.phoebe.test.model.EntityWithHashKeyAndRangeKey;


/**
 * @author Administrator
 * 
 */
public class TestMultiSave extends TestBase {
	private static final int NUM_OBJECTS_TO_SAVE = 10;
	private Phoebe phoebe = TestBase.PHOEBE;
	
	@Before
	public void setUp() {
		emptyAll();
	}
	
	@Test
	public void testMultiSaveObjectWithHashKey() {
		Set<String> hashKeySet = new HashSet<String>(NUM_OBJECTS_TO_SAVE);
		List<EntityWithHashKey> entityWithHashKeyList = new ArrayList<EntityWithHashKey>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String hashKey = new ObjectId().toString();
			hashKeySet.add(hashKey);
			EntityWithHashKey entityWithHashKey = new EntityWithHashKey();
			entityWithHashKey.setHashKey(hashKey);
			entityWithHashKey.setString(hashKey);
			entityWithHashKeyList.add(entityWithHashKey);
		}
		
		Iterable<SaveResult<EntityWithHashKey>> saveResults = phoebe.getDatastore().put(entityWithHashKeyList);
		int count = 0;
		for (SaveResult<EntityWithHashKey> saveResult : saveResults) {
			count++;
			Assert.assertTrue(saveResult.hasException() == false);
			EntityKey<EntityWithHashKey> key = EntityKey.create(saveResult.getObject());
			Assert.assertTrue(hashKeySet.contains(key.getHashKey()));
			Assert.assertTrue(key.getRangeKey() == null);
			Assert.assertTrue(key.getKindClass().equals(EntityWithHashKey.class));
		}
		Assert.assertTrue(count == NUM_OBJECTS_TO_SAVE);
		

		List<EntityWithHashKey> list = phoebe.getDatastore().get(EntityWithHashKey.class, hashKeySet);
		Assert.assertTrue(list.size() == NUM_OBJECTS_TO_SAVE);
	}


	@Test
	public void testMultiSaveObjectWithHashKeyAndRangeKey() {
		Set<String> hashKeyList = new HashSet<String>(NUM_OBJECTS_TO_SAVE);
		List<EntityWithHashKeyAndRangeKey> entityWithHashKeyAndRangeKeyList = new ArrayList<EntityWithHashKeyAndRangeKey>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String hashKey = new ObjectId().toString();
			hashKeyList.add(hashKey);
			EntityWithHashKeyAndRangeKey entityWithHashKeyAndRangeKey = new EntityWithHashKeyAndRangeKey();
			entityWithHashKeyAndRangeKey.setHashKey(hashKey);
			entityWithHashKeyAndRangeKey.setRangeKey(hashKey);
			entityWithHashKeyAndRangeKey.setString(hashKey);
			entityWithHashKeyAndRangeKeyList.add(entityWithHashKeyAndRangeKey);
		}
		
		Iterable<SaveResult<EntityWithHashKeyAndRangeKey>> saveResults = phoebe.getDatastore().put(entityWithHashKeyAndRangeKeyList);
		List<EntityKey<EntityWithHashKeyAndRangeKey>> keyList = new ArrayList<EntityKey<EntityWithHashKeyAndRangeKey>>();
		int count = 0;
		
		for (SaveResult<EntityWithHashKeyAndRangeKey> saveResult : saveResults) {
			count++;
			EntityKey<EntityWithHashKeyAndRangeKey> key = EntityKey.create(saveResult.getObject());
			keyList.add(key);
			Assert.assertTrue(hashKeyList.contains(key.getHashKey()));
			Assert.assertTrue(hashKeyList.contains(key.getRangeKey()));
			Assert.assertTrue(key.getKindClass().equals(EntityWithHashKeyAndRangeKey.class));
		}
		Assert.assertTrue(count == NUM_OBJECTS_TO_SAVE);
		
		List<EntityWithHashKeyAndRangeKey> list = phoebe.getDatastore().get(keyList);
		Assert.assertTrue(list.size() == NUM_OBJECTS_TO_SAVE);
		
	}
	
	@Test
	public void testNonHomogenous() {
		List<Object> objs = new ArrayList<Object>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String hashKey = new ObjectId().toString();
			EntityWithHashKey objectWithHashKey = new EntityWithHashKey();
			objectWithHashKey.setHashKey(hashKey);
			objectWithHashKey.setString(hashKey);
			objs.add(objectWithHashKey);
			
			
			EntityWithHashKeyAndRangeKey entityWithHashKeyAndRangeKey = new EntityWithHashKeyAndRangeKey();
			entityWithHashKeyAndRangeKey.setHashKey(hashKey);
			entityWithHashKeyAndRangeKey.setRangeKey(hashKey);
			entityWithHashKeyAndRangeKey.setString(hashKey);
			objs.add(entityWithHashKeyAndRangeKey);
		}
		
		Iterable<SaveResult<Object>> saveResults = phoebe.getDatastore().put(objs);
		List<EntityKey<Object>> keys = new ArrayList<EntityKey<Object>>();
		
		for (SaveResult<Object> saveResult : saveResults) {
			keys.add(EntityKey.create(saveResult.getObject()));
		}

		List<Object> read = phoebe.getDatastore().get(keys);
		Assert.assertTrue(read.size() == NUM_OBJECTS_TO_SAVE*2);
		
	}

}
