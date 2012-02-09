/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.result.SaveResult;
import com.swengle.phoebe.test.model.ObjectWithHashKey;
import com.swengle.phoebe.test.model.ObjectWithHashKeyAndRangeKey;

/**
 * @author Administrator
 * 
 */
public class TestMultiPut extends TestBase {
	private static final int NUM_OBJECTS_TO_SAVE = 10;
	private Phoebe phoebe = TestBase.PHOEBE;
	
	@Before
	public void setUp() {
		emptyAll();
	}
	
	@Test
	public void testMultiSaveObjectWithHashKey() {
		Set<String> objectIdList = new HashSet<String>(NUM_OBJECTS_TO_SAVE);
		List<ObjectWithHashKey> objectWithHashKeyList = new ArrayList<ObjectWithHashKey>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String id = new ObjectId().toString();
			objectIdList.add(id);
			ObjectWithHashKey objectWithHashKey = new ObjectWithHashKey();
			objectWithHashKey.setId(id);
			objectWithHashKey.setFoo(id);
			objectWithHashKeyList.add(objectWithHashKey);
		}
		
		Iterable<SaveResult<ObjectWithHashKey>> putResult = phoebe.getDatastore().put(objectWithHashKeyList);
		Iterator<SaveResult<ObjectWithHashKey>> iterator = putResult.iterator();
		int count = 0;
		while (iterator.hasNext()) {
			count++;
			SaveResult<ObjectWithHashKey> multiPutResult = iterator.next();
			Assert.assertTrue(multiPutResult.hasException() == false);
			EntityKey<ObjectWithHashKey> key = EntityKey.create(multiPutResult.getObject());
			Assert.assertTrue(objectIdList.contains(key.getHashKey()));
			Assert.assertTrue(key.getRangeKey() == null);
			Assert.assertTrue(key.getKindClass().equals(ObjectWithHashKey.class));
		}
		Assert.assertTrue(count == NUM_OBJECTS_TO_SAVE);
		

		List<ObjectWithHashKey> list = phoebe.getDatastore().get(ObjectWithHashKey.class, objectIdList);
		Assert.assertTrue(list.size() == NUM_OBJECTS_TO_SAVE);
	}


	@Test
	public void testMultiSaveObjectWithHashKeyAndRangeKey() {
		Set<String> objectIdList = new HashSet<String>(NUM_OBJECTS_TO_SAVE);
		List<ObjectWithHashKeyAndRangeKey> objectWithHashKeyAndRangeKeyList = new ArrayList<ObjectWithHashKeyAndRangeKey>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String id = new ObjectId().toString();
			objectIdList.add(id);
			ObjectWithHashKeyAndRangeKey objectWithHashKeyAndRangeKey = new ObjectWithHashKeyAndRangeKey();
			objectWithHashKeyAndRangeKey.setId(id);
			objectWithHashKeyAndRangeKey.setRangeId(id);
			objectWithHashKeyAndRangeKey.setFoo(id);
			objectWithHashKeyAndRangeKeyList.add(objectWithHashKeyAndRangeKey);
		}
		
		Iterable<SaveResult<ObjectWithHashKeyAndRangeKey>> putResult = phoebe.getDatastore().put(objectWithHashKeyAndRangeKeyList);
		Iterator<SaveResult<ObjectWithHashKeyAndRangeKey>> iterator = putResult.iterator();
		
		List<EntityKey<ObjectWithHashKeyAndRangeKey>> keyList = new ArrayList<EntityKey<ObjectWithHashKeyAndRangeKey>>();
		int count = 0;
		while (iterator.hasNext()) {
			count++;
			EntityKey<ObjectWithHashKeyAndRangeKey> key = EntityKey.create(iterator.next().getObject());
			keyList.add(key);
			Assert.assertTrue(objectIdList.contains(key.getHashKey()));
			Assert.assertTrue(objectIdList.contains(key.getRangeKey()));
			Assert.assertTrue(key.getKindClass().equals(ObjectWithHashKeyAndRangeKey.class));
		}
		Assert.assertTrue(count == NUM_OBJECTS_TO_SAVE);
		
		List<ObjectWithHashKeyAndRangeKey> list = phoebe.getDatastore().get(keyList);
		Assert.assertTrue(list.size() == NUM_OBJECTS_TO_SAVE);
		
	}
	
	@Test
	public void testNonHomogenous() {
		List<Object> objs = new ArrayList<Object>();
		for (int i=0; i<NUM_OBJECTS_TO_SAVE; i++) {
			String id = new ObjectId().toString();
			ObjectWithHashKey objectWithHashKey = new ObjectWithHashKey();
			objectWithHashKey.setId(id);
			objectWithHashKey.setFoo(id);
			objs.add(objectWithHashKey);
			
			
			ObjectWithHashKeyAndRangeKey objectWithHashKeyAndRangeKey = new ObjectWithHashKeyAndRangeKey();
			objectWithHashKeyAndRangeKey.setId(id);
			objectWithHashKeyAndRangeKey.setRangeId(id);
			objectWithHashKeyAndRangeKey.setFoo(id);
			objs.add(objectWithHashKeyAndRangeKey);
		}
		
		Iterable<SaveResult<Object>> putResult = phoebe.getDatastore().put(objs);
		List<EntityKey<Object>> keys = new ArrayList<EntityKey<Object>>();
		
		Iterator<SaveResult<Object>> putResultIterator = putResult.iterator();
		while (putResultIterator.hasNext()) {
			keys.add(EntityKey.create(putResultIterator.next().getObject()));
		}
		
		List<Object> read = phoebe.getDatastore().get(keys);
		Assert.assertTrue(read.size() == NUM_OBJECTS_TO_SAVE*2);
		
	}

}
