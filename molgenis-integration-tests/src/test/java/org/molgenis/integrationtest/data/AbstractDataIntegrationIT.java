package org.molgenis.integrationtest.data;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.molgenis.data.DataService;
import org.molgenis.data.elasticsearch.factory.EmbeddedElasticSearchServiceFactory;
import org.molgenis.data.meta.MetaDataServiceImpl;
import org.molgenis.data.transaction.AsyncTransactionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractDataIntegrationIT extends AbstractTestNGSpringContextTests
{
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	DataService dataService;

	@Autowired
	MetaDataServiceImpl metaDataService;

	@Autowired
	ConfigurableApplicationContext applicationContext;

	@Autowired
	EmbeddedElasticSearchServiceFactory embeddedElasticSearchServiceFactory;

	@Autowired
	AsyncTransactionLog asyncTransactionLog;

	@Autowired
	DataSource dataSource;

	// @BeforeClass
	// public void setUpBeforeClass() throws SQLException
	// {
	// Connection connection = dataSource.getConnection();
	// try
	// {
	// Statement statement = connection.createStatement();
	// try
	// {
	// statement.executeUpdate("CREATE DATABASE molgenistest;");
	// logger.debug("Created database molgenistest");
	// }
	// finally
	// {
	// statement.close();
	// }
	// }
	// finally
	// {
	// connection.close();
	// }
	// }

	@BeforeMethod
	public void setUpBeforeMethod() throws SQLException
	{
		String dbDriverClass = "org.postgresql.Driver";
		String dbJdbcUri = "jdbc:postgresql://localhost/";
		String dbUser = "postgres";
		String dbPassword = "P18kanjers";

		try
		{
			Class.forName(dbDriverClass);
		}
		catch (ClassNotFoundException e)
		{
			throw new RuntimeException(e);
		}

		Connection connection = DriverManager.getConnection(dbJdbcUri, dbUser, dbPassword);
		try
		{
			Statement statement = connection.createStatement();
			try
			{
				try
				{
					statement.executeUpdate("drop schema molgenistest;");
				}
				catch (SQLException e)
				{
					e.printStackTrace();
				}
				statement.executeUpdate("create schema molgenistest;");
			}
			finally
			{
				statement.close();
			}
		}
		finally
		{
			connection.close();
		}
		logger.debug("Created database [{}]", "molgenistest");
	}

	// @AfterClass
	// public void tearDownAfterClass() throws SQLException
	// {
	// Connection connection = dataSource.getConnection();
	// try
	// {
	// Statement statement = connection.createStatement();
	// try
	// {
	// statement.executeUpdate("DROP DATABASE molgenistest;");
	// logger.debug("Dropped database molgenistest");
	// }
	// finally
	// {
	// statement.close();
	// }
	// }
	// finally
	// {
	// connection.close();
	// }
	// }

	@AfterMethod
	public void tearDownAfterMethod() throws SQLException
	{

	}

	@BeforeClass
	public void init()
	{
		SecuritySupport.login();
	}

	@AfterClass
	public void cleanUp()
	{
		asyncTransactionLog.stop();

		try
		{
			// Give asyncTransactionLog time to stop gracefully
			TimeUnit.SECONDS.sleep(1);
		}
		catch (InterruptedException e)
		{
			logger.error("InterruptedException sleeping 1 second", e);
		}

		applicationContext.close();
		SecuritySupport.logout();

		try
		{
			// Stop ES
			embeddedElasticSearchServiceFactory.close();
		}
		catch (IOException e)
		{
			logger.error("Error stopping Elasticsearch", e);
		}

		try
		{
			// Delete molgenis home folder
			FileUtils.deleteDirectory(new File(System.getProperty("molgenis.home")));
		}
		catch (IOException e)
		{
			logger.error("Error removing molgenis home directory", e);
		}

	}
}
