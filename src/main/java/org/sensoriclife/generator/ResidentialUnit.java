package org.sensoriclife.generator;

import java.io.Serializable;

/**
 * 
 * @author paul
 * @version 0.0.2
 */
public class ResidentialUnit implements Serializable
{
  	private int electricityID;
  	private int electricityMeter;
		private int waterID;
  	private int hotWaterMeter;
		private int coldWaterMeter;
		private String address;
		private int persons;
		private int squareMeter;//not use for electricity, but later
	
	ResidentialUnit(int electricityID, String address, int persons)
	{
		this.electricityID = electricityID;
		this.electricityMeter = 0;
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
	
	public int getWaterID()
	{
		return waterID;
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
}
