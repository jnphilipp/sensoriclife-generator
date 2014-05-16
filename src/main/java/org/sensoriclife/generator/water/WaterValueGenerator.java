package org.sensoriclife.generator.water;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import org.sensoriclife.generator.world.ResidentialUnit;

/**
 * 
 * @author jnphilipp
 * @version 0.0.2
 */
public class WaterValueGenerator implements Serializable {
	/*
	 * Idea for later: put a number of waterId in an array and define ids which
	 * are in an office. For those, other calculations will call.
	 */
	public int generateNextValue(ResidentialUnit unit, Date timestamp, WaterType type) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);

		int consumption = 10;

		consumption *= getFactorForPersons(unit.getPersons());
		consumption *= getFactorForArea(unit.getSquareMeter());

		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY)
			consumption *= getFactorForWeekendDay(calendar);
		else
			consumption *= getFactorForWorkingDay(calendar);

		if (type == WaterType.COLD)
			return unit.getColdWaterMeter() + consumption;
		else
			return unit.getHotWaterMeter() + consumption;
	}

	/*
	 * calculates percentage consumption for the given time at a weekend day
	 * During the day the consumption has the same level.
	 * 
	 * @return double value beetween 0.0 and 1.0
	 */
	private double getFactorForWeekendDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 8 && hour < 9)
			return generateValueInRange(0.7, 0.8);
		else if (hour >= 9 && hour < 11)
			return generateValueInRange(0.0, 0.4);
		else if (hour >= 11 && hour < 13)
			return generateValueInRange(0.4, 0.5);
		else if (hour >= 13 && hour < 19)
			return generateValueInRange(0.0, 0.3);
		else if (hour >= 19 && hour < 22)
			return generateValueInRange(0.8, 1.0);
		else
			return generateValueInRange(0.0, 0.1);
	}

	/*
	 * calculates percentage consumption for the given time at a working day A
	 * time in the evening will return a value roughly 1.0 A time in during the
	 * day will return a value roughly 0.0
	 * 
	 * @return double value beetween 0.0 and 1.0
	 */
	private double getFactorForWorkingDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 6 && hour < 9)
			return generateValueInRange(0.7, 0.8);
		else if (hour >= 9 && hour < 18)
			return generateValueInRange(0.0, 0.4);
		else if (hour >= 18 && hour < 22)
			return generateValueInRange(0.8, 1.0);
		else
			return generateValueInRange(0.0, 0.4);
	}

	/*
	 * calculates percentage consumption for the given number of persons right
	 * now, its a really simple implementation and will multiply the consumption
	 * by the number of persons
	 */
	private double getFactorForPersons(int persons) {
		return persons;
	}
	
	/*
	 * calculates percentage consumption for the area of the unit.
	 * 10sm = 1.02
	 * 50sm = 1.1
	 * 100qm = 1.2
	 */	
	private double getFactorForArea(int squareMeter) {
		if (squareMeter < 1) 
			return 0;
		return 1 + (squareMeter / 500);
	}

	private double generateValueInRange(double min, double max) {
		return min + (Math.random() * (max - min));
	}
}
