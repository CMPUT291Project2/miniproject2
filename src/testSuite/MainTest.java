package testSuite;

import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import com.sleepycat.db.DatabaseException;

import project2.Main;

public class MainTest {

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	//@Test
	public void testSearchByKey() throws DatabaseException, IOException {
		Main main = new Main();
		
		main.createPopulateDB("btree");
		main.searchByKey();
		//main.searchByData();
		main.destroyDB();
		main.gatherAnswers();
	}
	
	@Test
	public void testSearchByData() throws DatabaseException, IOException {
		Main main = new Main();
		
		main.createPopulateDB("btree");
		main.searchByData();
		main.destroyDB();
		main.gatherAnswers();
	}

}
