package org.sensoriclife.generator.water;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import org.sensoriclife.generator.world.ResidentialUnit;

/**
 * 
 * @author jnphilipp
 * @version 0.0.3
 */
public class WaterValueGenerator implements Serializable {
	/*
	 * Idea for later: put a number of waterId in an array and define ids which
	 * are in an office. For those, other calculations will call.
	 */
	public float generateNextValue(ResidentialUnit unit, Date timestamp, WaterType type) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);

		float consumption = 10.0f;

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
	 * @return float value beetween 0.0 and 1.0
	 */
	private float getFactorForWeekendDay(Calendar calendar) {
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour > 8 && hour < 9)
			return generateValueInRange(0.7f, 0.8f);
		else if (hour >= 9 && hour < 11)
			return generateValueInRange(0.0f, 0.4f);
		else if (hour >= 11 && hour < 13)
			return generateValueInRange(0.4f, 0.5f);
		else if (hour >= 13 && hour < 19)
			return generateValueInRange(0.0f, 0.3f);
		else if (hour >= 19 && hour < 22)
			return generateValueInRange(0.8f, 1.0f);
		else
			return generateValueInRange(0.0f, 0.1f);
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
			return generateValueInRange(0.7f, 0.8f);
		else if (hour >= 9 && hour < 18)
			return generateValueInRange(0.0f, 0.4f);
		else if (hour >= 18 && hour < 22)
			return generateValueInRange(0.8f, 1.0f);
		else
			return generateValueInRange(0.0f, 0.4f);
	}

	/*
	 * calculates percentage consumption for the given number of persons right
	 * now, its a really simple implementation and will multiply the consumption
	 * by the number of persons. If nobody lives in the flat a small value will be returned
	 */
	private float getFactorForPersons(int persons) {
		if (persons < 1){
			float x = (float)Math.random();
			return (x < 0.1f)? x : 0;
		}
		return persons;
	}
	
	/*
	 * calculates percentage consumption for the area of the unit.
	 * 10sm = 1.02
	 * 50sm = 1.1
	 * 100qm = 1.2
	 */
	private float getFactorForArea(int squareMeter) {
		if (squareMeter < 1)
			return 0;
		return 1 + (squareMeter / 500);
	}

	private float generateValueInRange(float min, float max) {
		return min + ((float)Math.random() * (max - min));
	}
}
