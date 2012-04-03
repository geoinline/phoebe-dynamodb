/**
 * 
 */
package com.swengle.phoebe.test;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.swengle.phoebe.Phoebe;
import com.swengle.phoebe.key.EntityKey;
import com.swengle.phoebe.query.UpdateOperations;
import com.swengle.phoebe.test.model.EntityWithHashKeyAndLifecycleMethods;
import com.swengle.phoebe.test.model.EntityWithJustHashKeyAndLifecycleMethods;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class TestLifecycle extends TestBase {
	private Phoebe phoebe = TestBase.PHOEBE;
	public static Set<String> deletedEntityHashKeys = new HashSet<String>();
	public static Set<String> updatedEntityHashKeys = new HashSet<String>();
	
	
	
	@Before
	public void setUp() {
		emptyAll();
	}

	@Test
	public void testLifecycleForEntityWithJustHashKey() {
		EntityWithJustHashKeyAndLifecycleMethods entity = new EntityWithJustHashKeyAndLifecycleMethods();
		entity.setHashKey(new ObjectId().toString());
		phoebe.getDatastore().insert(entity);
		Assert.assertTrue(entity.isCreated());
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		entity.setCreated(false);
		
		phoebe.getDatastore().put(entity);
		Assert.assertTrue(entity.isCreated() == false);
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		updatedEntityHashKeys.clear();
		
		entity = phoebe.getDatastore().get(EntityWithJustHashKeyAndLifecycleMethods.class, entity.getHashKey());
		Assert.assertTrue(entity.isCreated() == false);
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == true);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		
		phoebe.getDatastore().delete(EntityWithJustHashKeyAndLifecycleMethods.class, entity.getHashKey());
		Assert.assertTrue(deletedEntityHashKeys.contains(entity.getHashKey()));
		deletedEntityHashKeys.clear();
		
		phoebe.getDatastore().delete(EntityWithJustHashKeyAndLifecycleMethods.class, entity.getHashKey());
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
	
		entity = new EntityWithJustHashKeyAndLifecycleMethods();
		entity.setHashKey(new ObjectId().toString());
		phoebe.getDatastore().put(entity);
		Assert.assertTrue(entity.isCreated());
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
	}
	
	@Test
	public void testLifecycleForEntityWithHashKey() {
		EntityWithHashKeyAndLifecycleMethods entity = new EntityWithHashKeyAndLifecycleMethods();
		entity.setHashKey(new ObjectId().toString());
		phoebe.getDatastore().insert(entity);
		Assert.assertTrue(entity.isCreated());
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		entity.setCreated(false);
		
		phoebe.getDatastore().put(entity);
		Assert.assertTrue(entity.isCreated() == false);
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(updatedEntityHashKeys.contains(entity.getHashKey()));
		updatedEntityHashKeys.clear();
		
		UpdateOperations<EntityWithHashKeyAndLifecycleMethods> ops = phoebe.createUpdateOperations(EntityWithHashKeyAndLifecycleMethods.class);
		ops.set("string", "foo");
		phoebe.getDatastore().update(EntityKey.create(entity), ops);
		Assert.assertTrue(entity.isCreated() == false);
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(updatedEntityHashKeys.contains(entity.getHashKey()));
		updatedEntityHashKeys.clear();
		
		entity = phoebe.getDatastore().get(EntityWithHashKeyAndLifecycleMethods.class, entity.getHashKey());
		Assert.assertTrue(entity.isCreated() == false);
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == true);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		
		entity = new EntityWithHashKeyAndLifecycleMethods();
		entity.setHashKey(new ObjectId().toString());
		phoebe.getDatastore().put(entity);
		Assert.assertTrue(entity.isCreated());
		Assert.assertTrue(!deletedEntityHashKeys.contains(entity.getHashKey()));
		Assert.assertTrue(entity.isRead() == false);
		Assert.assertTrue(!updatedEntityHashKeys.contains(entity.getHashKey()));
		
	}

}
