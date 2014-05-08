package org.sensoriclife.generator;

import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import static org.junit.Assert.*;
import org.junit.Test;
import org.sensoriclife.Logger;
import org.sensoriclife.db.Accumulo;

/**
 *
 * @author paul
 */
public class WorldGeneratorTest {
	/**
	 * Test of createWorld method, of class WorldGenerator.
	 */
	@Test
	public void testCeateWorld() {
		App.loadConfig();

		try {
			Accumulo.getInstance().connect();
			Accumulo.getInstance().createTable("generator_helper_table");
		} catch ( AccumuloException | AccumuloSecurityException e ) {
			Logger.error("Error while connecting to accumulo.", e.toString());
		} catch ( TableExistsException e ) {
			Logger.error("Error while creating table.", e.toString());
		}

		WorldGenerator instance = new WorldGenerator();
		instance.createWorld();

		try {
			Iterator<Entry<Key, Value>> entries = Accumulo.getInstance().scannAll("generator_helper_table");
			int i = 0;
			while ( entries.hasNext() ) {
				Entry<Key, Value> entry = entries.next();
				i++;
			}

			assertNotEquals(i, 0);
		}
		catch ( TableNotFoundException e ) {
			Logger.error("Error while reading data.", e.toString());
		}
	}	
}