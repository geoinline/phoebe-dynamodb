/**
 * 
 */
package com.swengle.phoebe.test.model;

import com.amazonaws.services.dynamodb.datamodeling.DynamoDBIgnore;
import com.swengle.phoebe.annotation.OnCreate;
import com.swengle.phoebe.annotation.OnDelete;
import com.swengle.phoebe.annotation.OnRead;
import com.swengle.phoebe.annotation.OnUpdate;
import com.swengle.phoebe.test.TestLifecycle;

/**
 * @author Brian O'Connor <btoc008@gmail.com>
 *
 */
public class EntityWithJustHashKeyAndLifecycleMethods extends EntityWithJustHashKey {
	private boolean created;
	private boolean read;


	/**
	 * 
	 */
	public EntityWithJustHashKeyAndLifecycleMethods() {
		// nothing to do
	}
	
	/**
	 * @return the created
	 */
	@DynamoDBIgnore
	public boolean isCreated() {
		return created;
	}


	/**
	 * @param created the created to set
	 */
	public void setCreated(boolean created) {
		this.created = created;
	}


	/**
	 * @return the read
	 */
	@DynamoDBIgnore
	public boolean isRead() {
		return read;
	}


	/**
	 * @param read the read to set
	 */
	public void setRead(boolean read) {
		this.read = read;
	}

	
	
	@OnCreate
	public void onCreate() {
		created = true;
	}
	
	@OnUpdate
	public void onUpdate() {
		TestLifecycle.updatedEntityHashKeys.add(getHashKey());
	}
	
	@OnRead
	public void onRead() {
		read = true;
	}
	
	@OnDelete
	public void onDelete() {
		TestLifecycle.deletedEntityHashKeys.add(getHashKey());
	}
	

}
