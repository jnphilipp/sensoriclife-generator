package org.sensoriclife.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.sensoriclife.Logger;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.2
 */
public class App {
	/**
	 * default database configuration file
	 */
	private static final String DEFAULT_CONFIGURATION_FILE = Helpers.getUserDir() + "/config/config.properties";
	/**
	 * properties
	 */
	private static Properties properties;

	public static void main(String[] args) {
		String configFile = App.DEFAULT_CONFIGURATION_FILE, logFile = "";
		boolean world = false, electricity = false;

		if ( args.length != 0 ) {
			List<String> l = Arrays.asList(args);
			Iterator<String> it = l.iterator();

			while ( it.hasNext() ) {
				switch ( it.next() ) {
					case "-conf": {
						configFile = it.next();
					} break;
					case "-log": {
						logFile = it.next();
					} break;
					case "world": {
						world = true;
					} break;
					case "electricity": {
						electricity = true;
					} break;
				}
			}
		}

		if ( !logFile.isEmpty() && new File(logFile).exists() )
			try {
				Logger.getInstance(logFile);
			}
			catch ( IOException e ) {
				Logger.error(App.class, "Error while reading log config file.", e.toString());
				System.exit(1);
			}
		else
			Logger.getInstance();

		App.loadConfig(configFile);

		if ( world )
			new WorldGenerator().run();
		if ( electricity )
			new ElectricityGenerator().run();
	}

	/**
	 * @return the properties
	 */
	public static Properties getProperties() {
		return App.properties;
	}

	/**
	 * @param key key
	 * @return values of the given key
	 */
	public static String getProperty(String key) {
		switch ( key ) {
			case "realtime":
				return App.properties.getProperty(key, "true");
			default:
				return App.properties.getProperty(key, "");
		}
	}

	/**
	 * Returns the value of the given key as boolean;
	 * @param key key
	 * @return <code>true</code> or <code>false</code>
	 */
	public static boolean getBooleanProperty(String key) {
		return Boolean.valueOf(App.getProperty(key));
	}

	/**
	 * Returns the value of the given key as integer.
	 * @param key key
	 * @return integer value
	 */
	public static int getIntegerProperty(String key) {
		return Integer.parseInt(App.getProperty(key));
	}

	public static void loadConfig(String configFile) {
		if ( !new File(configFile).exists() ) {
			System.err.println("The configuration file does not exists.");
			Logger.error(App.class, "The configuration file does not exists.");
			System.exit(1);
		}

		try {
			App.properties = new Properties();
			App.properties.load(new FileInputStream(configFile));
		}
		catch ( IOException e ) {
			Logger.error(App.class, "Error while reading config file.", e.toString());
			System.exit(1);
		}
	}
}