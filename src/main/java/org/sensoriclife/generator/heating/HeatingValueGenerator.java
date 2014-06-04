package org.sensoriclife.generator.heating;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import org.sensoriclife.generator.world.ResidentialUnit;

/**
 * 
 * @author stefan, jnphilipp
 * @version 0.0.5
 */
public class HeatingValueGenerator implements Serializable {
	/*
	 * Idea for later: put a number of heatingID in an array and define ids
	 * which are in an office. For those, other calculations will call.
	 */
	public float[] generateNextValue(ResidentialUnit unit, Date timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);

		float consumption = 100;
		consumption *= getFactorForMonth(calendar);
		consumption *= getFactorForPersons(unit.getPersons());
		consumption *= getFactorForArea(unit.getSquareMeter());

		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY)
			consumption *= getFactorForWeekendDay(calendar);
		else
			consumption *= getFactorForWorkingDay(calendar);

		float[] heatingMeters = unit.getHeatingMeters();
		for ( int i = 0; i < heatingMeters.length; i++ )
			heatingMeters[i] += consumption;
		return heatingMeters;
	}

	/*
	 * calculates percentage consumption for a month e.g. a month in winter will
	 * return a consumption roughly 1.0
	 * 
	 * @return float value beetween 0.0 and 1.0
	 */
	private float getFactorForMonth(Calendar calendar) {
		switch (calendar.get(Calendar.MONTH)) {
		case Calendar.JANUARY:
			return generateValueInRange(0.9f, 1.0f);
		case Calendar.FEBRUARY:
			return generateValueInRange(0.9f, 1.0f);
		case Calendar.MARCH:
			return generateValueInRange(0.8f, 0.9f);
		case Calendar.APRIL:
			return generateValueInRange(0.6f, 0.8f);
		case Calendar.MAY:
			return generateValueInRange(0.2f, 0.6f);
		case Calendar.JUNE:
			return generateValueInRange(0.0f, 0.2f);
		case Calendar.JULY:
			return generateValueInRange(0.0f, 0.1f);
		case Calendar.AUGUST:
			return generateValueInRange(0.0f, 0.1f);
		case Calendar.SEPTEMBER:
			return generateValueInRange(0.2f, 0.6f);
		case Calendar.OCTOBER:
			return generateValueInRange(0.6f, 0.8f);
		case Calendar.NOVEMBER:
			return generateValueInRange(0.8f, 0.9f);
		default:
			return generateValueInRange(0.9f, 1.0f);
		}
	}

	/*
	 * calculates percentage consumption for the given time at a weekend day
	 * During the day the consumption has the same level.
	 * 
	 * @return float value beetween 0.0 and 1.0
	 */
	private float getFactorForWeekendDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 6 && hour < 23)
			return generateValueInRange(0.5f, 1.0f);
		else
			return generateValueInRange(0.0f, 0.4f);
	}

	/*
	 * calculates percentage consumption for the given time at a working day A
	 * time in the evening will return a value roughly 1.0 A time in during the
	 * day will return a value roughly 0.0
	 * 
	 * @return float value beetween 0.0 and 1.0
	 */
	private float getFactorForWorkingDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 6 && hour < 9)
			return generateValueInRange(0.5f, 0.8f);
		else if (hour >= 9 && hour < 18)
			return generateValueInRange(0.0f, 0.4f);
		else if (hour >= 18 && hour < 22)
			return generateValueInRange(0.8f, 1.0f);
		else
			return generateValueInRange(0.0f, 0.4f);
	}

	/*
	 * calculates percentage consumption for the area of the unit.
	 * 10sm = 1
	 * 50sm = 1,7
	 * 100qm = 2
	 */
	private float getFactorForArea(int squareMeter) {
		if (squareMeter < 1)
			return 0;
		return (float)Math.log(squareMeter);
	}

	/*
	 * calculates percentage consumption for the given number of persons.
	 * If nobody lives in the flat, a small value will be returned (or 0). Otherwise 1
	 */
	private float getFactorForPersons(int persons) {
		if (persons < 1){
			float x = (float)Math.random();
			return (x < 0.1f)? x : 0;
		}
		return 1;
	}

	private float generateValueInRange(float min, float max) {
		return min + ((float)Math.random() * (max - min));
	}
}