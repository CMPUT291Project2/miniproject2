package testSuite;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import project2.Main;

import com.sleepycat.db.DatabaseException;

public class MainTest {

	//@Test
	public void test() {
		fail("Not yet implemented");
	}

	//@Test
	public void testSearchByKeyBTree() throws DatabaseException, IOException {
		Main main = new Main();

		main.createPopulateDB("btree");
		boolean success = main.searchByKey();
		//main.searchByData();
		main.destroyDB();
		assertTrue(success);
	}

	//@Test
	public void testSearchByKeyHash() throws DatabaseException, IOException {
		Main main = new Main();

		main.createPopulateDB("hash");
		boolean success = main.searchByKey();
		//main.searchByData();
		main.destroyDB();
		assertTrue(success);
	}

	//@Test
	public void testSearchByDataBTree() throws DatabaseException, IOException {
		Main main = new Main();

		System.out.println("B Tree Structure");
		main.createPopulateDB("btree");
		main.searchByDataBTree();
		//main.searchByData();
		main.destroyDB();
	}

	//@Test
	public void testSearchByDataHash() throws DatabaseException, IOException {
		Main main = new Main();

		System.out.println("Hash Structure");
		main.createPopulateDB("hash");
		main.searchByDataHash();
		//main.searchByData();
		main.destroyDB();
	}
	
	@Test
	public void testSearchByKeyRangeBTree() throws DatabaseException, IOException {

		Main main = new Main();

		System.out.println("Hash Structure");
		main.createPopulateDB("hash");
		main.searchByKeyRangeBTree();
		//main.searchByData();
		main.destroyDB();
	}



}
