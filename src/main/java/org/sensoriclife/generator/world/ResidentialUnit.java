package org.sensoriclife.generator.world;

import java.io.Serializable;


/**
 * 
 * @author paul jnphilipp
 * @version 0.1.0
 */
public class ResidentialUnit implements Serializable {
	private long electricityID;
	private float electricityMeter;
	private long hotWaterID;
	private long coldWaterID;
	private float hotWaterMeter;
	private float coldWaterMeter;
	private long[] heatingIDs;
	private float[] heatingMeters;
	private String address;
	private int persons;
	private int squareMeter;//not use for electricity, but later

	ResidentialUnit(long electricityID, long hotWaterID, long coldWaterID, long[] heatingID, String address, int persons) {
		this.electricityID = electricityID;
		this.hotWaterID = hotWaterID;
		this.coldWaterID = coldWaterID;
		this.heatingIDs = heatingID;

		this.electricityMeter = 0;
		this.hotWaterMeter = 0;
		this.coldWaterMeter = 0;

		this.heatingMeters = new float[this.heatingIDs.length];
		for ( int i = 0; i < this.heatingIDs.length; i++ )
			this.heatingMeters[i] = 0;

		this.address = address;
		this.persons = persons;
		this.squareMeter = (this.persons + 1) * 25;
	}

	public long getElectricityID() {
		return this.electricityID;
	}

	public float getElectricityMeter() {
		return this.electricityMeter;
	}

	public void setElectricityMeter(float newValue) {
		this.electricityMeter = newValue;
	}

	public String getAddress() {
		return this.address;
	}

	public int getPersons() {
		return this.persons;
	}

	public int getSquareMeter() {
		return this.squareMeter;
	}

	public long getHotWaterID() {
		return this.hotWaterID;
	}

	public long getColdWaterID() {
		return this.coldWaterID;
	}

	public float getHotWaterMeter() {
		return this.hotWaterMeter;
	}

	public void setHotWaterMeter(float newValue) {
		this.hotWaterMeter = newValue;
	}

	public float getColdWaterMeter() {
		return this.coldWaterMeter;
	}

	public void setColdWaterMeter(float newValue) {
		this.coldWaterMeter = newValue;
	}

	public long[] getHeatingIDs() {
		return this.heatingIDs;
	}

	public float[] getHeatingMeters() {
		return this.heatingMeters;
	}

	public void setHeatingMeters(float[] newValues) {
		this.heatingMeters = newValues;
	}
}