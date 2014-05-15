package org.sensoriclife.generator.world;

import java.io.Serializable;


/**
 * 
 * @author paul jnphilipp
 * @version 0.0.6
 */
public class ResidentialUnit implements Serializable
{
  	private int electricityID;
  	private int electricityMeter;
		private int hotWaterID;
		private int coldWaterID;
  	private int hotWaterMeter;
		private int coldWaterMeter;
		private int[] heatingIDs;
  	private int[] heatingMeters;
		private String address;
		private int persons;
		private int squareMeter;//not use for electricity, but later
	
	ResidentialUnit(int electricityID, int hotWaterID, int coldWaterID, int[] heatingID, String address, int persons) {
		this.electricityID = electricityID;
		this.hotWaterID = hotWaterID;
		this.coldWaterID = coldWaterID;
		this.heatingIDs = heatingID;

		this.electricityMeter = 0;
		this.hotWaterMeter = 0;
		this.coldWaterMeter = 0;

		this.heatingMeters = new int[this.heatingIDs.length];
		for(int i=0;i<this.heatingIDs.length;i++)
			this.heatingMeters[i] = 0;

		this.address = address;	
		this.persons = persons;
		this.squareMeter = this.persons+1*25;
	}
	
	public int getElectricityID()
	{
		return electricityID;
	}
	
	public int getElectricityMeter()
	{
		return electricityMeter;
	}
	
	public void setElectricityMeter(int newValue)
	{
		this. electricityMeter = newValue;
	}
	
	public String getAddress()
	{
		return address;
	}
	
	public int getPersons()
	{
		return persons;
	}
	
	public int getSquareMeter()
	{
		return squareMeter;
	}	
	
	public int getHotWaterID()
	{
		return hotWaterID;
	}
	
	public int getColdWaterID()
	{
		return coldWaterID;
	}
	
	public int getHotWaterMeter()
	{
		return hotWaterMeter;
	}
	
	public void setHotWaterMeter(int newValue)
	{
		this.hotWaterMeter=newValue;
	}
	
	public int getColdWaterMeter()
	{
		return coldWaterMeter;
	}
	
	public void setColdWaterMeter(int newValue)
	{
		this.coldWaterMeter=newValue;
	}
	
	public int[] getHeatingIDs()
	{
		return heatingIDs;
	}
	
	public int[] getHeatingMeters()
	{
		return heatingMeters;
	}
	
	public void setHeatingMeters(int[] newValue)
	{
		this.heatingMeters=newValue;
	}
}
