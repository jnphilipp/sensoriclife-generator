package org.sensoriclife.generator.world;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 * @author paul
 * @version 0.0.4
 */
public class User implements Serializable
{
  private long id;
	private String name;
	private ArrayList<String> address = new ArrayList<String>();

	User(long id, String address) {
		this.id = id;
		this.name = NameGenerator.getName();
		this.address.add(address);	
	}
	
	public long getID() {
		return this.id;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public void addAddress(String value)
	{
		this.address.add(value);
	}
	
	public ArrayList<String> getAddresses()
	{
		return this.address;
	}

	public List<String> getOtherAddresses() {
		if ( this.address.size() > 1 )
			return this.address.subList(1, this.address.size());
		else
			return new ArrayList<>();
	}
	
	public String getBillingAddress()
	{
		return this.address.get(0);
	}
}
