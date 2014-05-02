package org.sensoriclife.generator;

import java.io.Serializable;

public class ResidentialUnit implements Serializable
{
  	private int electricityID;
  	private int electricityMeter;
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
}
