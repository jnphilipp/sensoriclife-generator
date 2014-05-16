package org.sensoriclife.generator.electricity;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import org.sensoriclife.generator.world.ResidentialUnit;

public class ElectricityValueGenerator implements Serializable {

	/*
	 * Idea for later: put a number of electroID in an array and define ids
	 * which are in an office. For those, other calculations will call.
	 */
	public int generateNextValue(ResidentialUnit unit, Date timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);

		int consumption = 100;
		consumption *= getFactorForMonth(calendar);
		consumption *= getFactorForPersons(unit.getPersons());
		consumption *= getFactorForArea(unit.getSquareMeter());

		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY)
			consumption *= getFactorForWeekendDay(calendar);
		else
			consumption *= getFactorForWorkingDay(calendar);

		return unit.getElectricityMeter() + consumption;
	}

	/*
	 * calculates percentage consumption for a month e.g. a month in winter and
	 * summer will return a high value because in winter you need light the
	 * whole day and in summer the air conditioning
	 * 
	 * @return double value beetween 0.0 and 1.0
	 */
	private double getFactorForMonth(Calendar calendar) {
		switch (calendar.get(Calendar.MONTH)) {
		case Calendar.JANUARY:
			return generateValueInRange(0.9, 1);
		case Calendar.FEBRUARY:
			return generateValueInRange(0.9, 1);
		case Calendar.MARCH:
			return generateValueInRange(0.8, 0.9);
		case Calendar.APRIL:
			return generateValueInRange(0.6, 0.8);
		case Calendar.MAY:
			return generateValueInRange(0.5, 0.7);
		case Calendar.JUNE:
			return generateValueInRange(0.6, 0.7);
		case Calendar.JULY:
			return generateValueInRange(0.7, 0.8);
		case Calendar.AUGUST:
			return generateValueInRange(0.7, 0.8);
		case Calendar.SEPTEMBER:
			return generateValueInRange(0.5, 0.6);
		case Calendar.OCTOBER:
			return generateValueInRange(0.6, 0.8);
		case Calendar.NOVEMBER:
			return generateValueInRange(0.8, 0.9);
		default:
			return generateValueInRange(0.9, 1);
		}
	}

	/*
	 * calculates percentage consumption for the given time at a weekend day
	 * During the day the consumption has the same level.
	 * 
	 * @return double value beetween 0.0 and 1.0
	 */
	private double getFactorForWeekendDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 6 && hour < 23)
			return generateValueInRange(0.5, 1.0);
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
			return generateValueInRange(0.5, 0.8);
		else if (hour >= 9 && hour < 18)
			return generateValueInRange(0.1, 0.3);
		else if (hour >= 18 && hour < 22)
			return generateValueInRange(0.8, 1.0);
		else
			return generateValueInRange(0.0, 0.1);
	}

	/*
	 * calculates percentage consumption for the given number of persons. Two
	 * persons need more energy than one but not by factor 2...
	 */
	private double getFactorForPersons(int persons) {
		if (persons < 1)
			return 0;
		return 1 + Math.log(persons);
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
