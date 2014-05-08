package org.sensoriclife.generator.heating;

import java.util.Calendar;
import java.util.Date;

public class HeatingValueGenerator {
	/*
	 * Idea for later: put a number of heatingID in an array and define ids
	 * which are in an office. For those, other calculations will call.
	 */
	public int generateNextValue(int heatingID, int heatingMeter, Date timestamp) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timestamp);

		int consumption = 100;
		consumption *= getFactorForMonth(calendar);

		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY)
			consumption *= getFactorForWeekendDay(calendar);
		else
			consumption *= getFactorForWorkingDay(calendar);

		return heatingMeter + consumption;
	}

	/*
	 * calculates percentage consumption for a month e.g. a month in winter will
	 * return a consumption roughly 1.0
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
			return generateValueInRange(0.2, 0.6);
		case Calendar.JUNE:
			return generateValueInRange(0, 0.2);
		case Calendar.JULY:
			return generateValueInRange(0.0, 0.1);
		case Calendar.AUGUST:
			return generateValueInRange(0.0, 0.1);
		case Calendar.SEPTEMBER:
			return generateValueInRange(0.2, 0.6);
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
			return generateValueInRange(0.0, 0.4);
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
			return generateValueInRange(0.0, 0.4);
		else if (hour >= 18 && hour < 22)
			return generateValueInRange(0.8, 1.0);
		else
			return generateValueInRange(0.0, 0.4);
	}

	private double generateValueInRange(double min, double max) {
		return min + (Math.random() * (max - min));
	}

}
