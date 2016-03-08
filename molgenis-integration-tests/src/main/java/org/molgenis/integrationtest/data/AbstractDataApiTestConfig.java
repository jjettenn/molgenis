package org.molgenis.integrationtest.data;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.molgenis.data.EntityManager;
import org.molgenis.data.EntityManagerImpl;
import org.molgenis.data.IdGenerator;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryDecoratorFactory;
import org.molgenis.data.elasticsearch.ElasticsearchEntityFactory;
import org.molgenis.data.elasticsearch.ElasticsearchRepositoryCollection;
import org.molgenis.data.elasticsearch.SearchService;
import org.molgenis.data.elasticsearch.config.EmbeddedElasticSearchConfig;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.data.meta.MetaDataServiceImpl;
import org.molgenis.data.settings.AppSettings;
import org.molgenis.data.support.DataServiceImpl;
import org.molgenis.data.support.OwnedEntityMetaData;
import org.molgenis.data.support.UuidGenerator;
import org.molgenis.data.transaction.MolgenisTransactionManager;
import org.molgenis.data.transaction.TransactionConfig;
import org.molgenis.data.transaction.TransactionLogService;
import org.molgenis.data.validation.EntityAttributesValidator;
import org.molgenis.data.validation.ExpressionValidator;
import org.molgenis.file.FileMetaMetaData;
import org.molgenis.js.RhinoConfig;
import org.molgenis.security.core.MolgenisPasswordEncoder;
import org.molgenis.security.core.runas.RunAsSystemBeanPostProcessor;
import org.molgenis.security.permission.PermissionSystemService;
import org.molgenis.ui.MolgenisRepositoryDecoratorFactory;
import org.molgenis.ui.RepositoryDecoratorRegistry;
import org.molgenis.util.ApplicationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import com.google.common.io.Files;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@EnableTransactionManagement(proxyTargetClass = true)
@ComponentScan(
{ "org.molgenis.data.meta", "org.molgenis.data.elasticsearch.index", "org.molgenis.auth" })
@Import(
{ EmbeddedElasticSearchConfig.class, ElasticsearchEntityFactory.class, TransactionConfig.class,
		ElasticsearchRepositoryCollection.class, RunAsSystemBeanPostProcessor.class, FileMetaMetaData.class,
		OwnedEntityMetaData.class, RhinoConfig.class, ExpressionValidator.class, LanguageService.class })
public abstract class AbstractDataApiTestConfig
{
	private static final Logger LOG = LoggerFactory.getLogger(AbstractDataApiTestConfig.class);

	@Autowired
	protected SearchService searchService;

	@Autowired
	private TransactionLogService transactionLogService;

	@Autowired
	public ExpressionValidator expressionValidator;

	// private String dbName;

	protected AbstractDataApiTestConfig()
	{
		System.setProperty("molgenis.home", Files.createTempDir().getAbsolutePath());
	}

	@PostConstruct
	public void init() throws SQLException
	{
		// if (this.dbName == null)
		// {
		// this.dbName = "test_" + idGenerator().generateId();
		// createDatabase(dbName);
		// }

		dataService().setMeta(metaDataService());
		metaDataService().setDefaultBackend(getBackend());
	}

	// private void createDatabase(String dbName) throws SQLException
	// {
	// String dbDriverClass = "org.postgresql.Driver";
	// String dbJdbcUri = "jdbc:postgresql://localhost/";
	// String dbUser = "postgres";
	// String dbPassword = "P18kanjers";
	//
	// try
	// {
	// Class.forName(dbDriverClass);
	// }
	// catch (ClassNotFoundException e)
	// {
	// throw new RuntimeException(e);
	// }
	//
	// Connection connection = DriverManager.getConnection(dbJdbcUri, dbUser, dbPassword);
	// try
	// {
	// Statement statement = connection.createStatement();
	// try
	// {
	// statement.executeUpdate("CREATE DATABASE " + dbName + ";");
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
	// LOG.debug("Created database [{}]", dbName);
	// }

	protected abstract ManageableRepositoryCollection getBackend();

	@Bean
	public MetaDataService metaDataService()
	{
		return new MetaDataServiceImpl(dataService());
	}

	@Bean
	public LanguageService languageService()
	{
		return new LanguageService(dataService(), appSettings());
	}

	@Bean
	public IdGenerator idGenerator()
	{
		return new UuidGenerator();
	}

	@Bean
	public MolgenisTransactionManager transactionManager()
	{
		return new MolgenisTransactionManager(idGenerator(), dataSource());
	}

	@Bean
	public DataServiceImpl dataService()
	{
		return new DataServiceImpl(repositoryDecoratorFactory());
	}

	@Bean
	public EntityManager entityManager()
	{
		return new EntityManagerImpl(dataService());
	}

	@Bean
	public PermissionSystemService permissionSystemService()
	{
		return new PermissionSystemService(dataService());
	}

	@Bean
	public AppSettings appSettings()
	{
		return new TestAppSettings();
	}

	@Bean
	public EntityAttributesValidator entityAttributesValidator()
	{
		return new EntityAttributesValidator();
	}

	@Bean
	public RepositoryDecoratorRegistry repositoryDecoratorRegistry()
	{
		return new RepositoryDecoratorRegistry();
	}

	@Bean
	public RepositoryDecoratorFactory repositoryDecoratorFactory()
	{
		return new RepositoryDecoratorFactory()
		{
			@Override
			public Repository createDecoratedRepository(Repository repository)
			{
				return new MolgenisRepositoryDecoratorFactory(entityManager(), transactionLogService,
						entityAttributesValidator(), idGenerator(), appSettings(), dataService(), expressionValidator,
						repositoryDecoratorRegistry()).createDecoratedRepository(repository);
			}
		};
	}

	@Bean(destroyMethod = "close")
	// public DataSource dataSource()
	// {
	// return new EmbeddedMysqlDatabaseBuilder().build();
	// }
	public ComboPooledDataSource dataSource()
	{
		// if (this.dbName == null)
		// {
		// this.dbName = "test_" + idGenerator().generateId();
		// try
		// {
		// createDatabase(this.dbName);
		// try
		// {
		// Thread.sleep(1000);
		// }
		// catch (InterruptedException e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// catch (SQLException e)
		// {
		// throw new RuntimeException(e);
		// }
		// }
		// this.dbName = "test_" + idGenerator().generateId();
		// try
		// {
		// createDatabase(dbName);
		// }
		// catch (SQLException e)
		// {
		// throw new RuntimeException(e);
		// }

		String dbDriverClass = "org.postgresql.Driver";
		String dbJdbcUri = "jdbc:postgresql://localhost/molgenistest";
		String dbUser = "postgres";
		String dbPassword = "P18kanjers";
		// if(dbDriverClass == null) throw new IllegalArgumentException("db_driver is null");
		// if(dbJdbcUri == null) throw new IllegalArgumentException("db_uri is null");
		// if(dbUser == null) throw new IllegalArgumentException("please configure the db_user property in your
		// molgenis-server.properties");
		// if(dbPassword == null) throw new IllegalArgumentException("please configure the db_password property in your
		// molgenis-server.properties");

		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		try
		{
			dataSource.setDriverClass(dbDriverClass);
		}
		catch (PropertyVetoException e)
		{
			throw new RuntimeException(e);
		}
		dataSource.setJdbcUrl(dbJdbcUri);
		dataSource.setUser(dbUser);
		dataSource.setPassword(dbPassword);
		dataSource.setMinPoolSize(5);
		// dataSource.setMaxPoolSize(150);
		dataSource.setMaxPoolSize(100);
		dataSource.setTestConnectionOnCheckin(true);
		dataSource.setIdleConnectionTestPeriod(120);
		return dataSource;
		// return new DatabaseRemovingDataSource(dataSource, this.dbName);
		// return new EmbeddedMysqlDatabaseBuilder().build();
	}

	// public static class DatabaseRemovingDataSource implements DataSource
	// {
	// private final ComboPooledDataSource dataSource;
	// private final String dbName;
	//
	// public DatabaseRemovingDataSource(ComboPooledDataSource dataSource, String dbName)
	// {
	// this.dataSource = dataSource;
	// this.dbName = dbName;
	// }
	//
	// @Override
	// public PrintWriter getLogWriter() throws SQLException
	// {
	// return dataSource.getLogWriter();
	// }
	//
	// @Override
	// public void setLogWriter(PrintWriter out) throws SQLException
	// {
	// dataSource.setLogWriter(out);
	// }
	//
	// @Override
	// public void setLoginTimeout(int seconds) throws SQLException
	// {
	// dataSource.setLoginTimeout(seconds);
	// }
	//
	// @Override
	// public int getLoginTimeout() throws SQLException
	// {
	// return dataSource.getLoginTimeout();
	// }
	//
	// @Override
	// public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
	// {
	// return dataSource.getParentLogger();
	// }
	//
	// @Override
	// public <T> T unwrap(Class<T> iface) throws SQLException
	// {
	// return dataSource.unwrap(iface);
	// }
	//
	// @Override
	// public boolean isWrapperFor(Class<?> iface) throws SQLException
	// {
	// return dataSource.isWrapperFor(iface);
	// }
	//
	// @Override
	// public Connection getConnection() throws SQLException
	// {
	// return dataSource.getConnection();
	// }
	//
	// @Override
	// public Connection getConnection(String username, String password) throws SQLException
	// {
	// return dataSource.getConnection(username, password);
	// }
	//
	// public void close() throws SQLException
	// {
	// dropDatabase();
	//
	// dataSource.close();
	// }
	//
	// private void dropDatabase() throws SQLException
	// {
	// String dbDriverClass = "org.postgresql.Driver";
	// String dbJdbcUri = "jdbc:postgresql://localhost/";
	// String dbUser = "postgres";
	// String dbPassword = "P18kanjers";
	//
	// try
	// {
	// Class.forName(dbDriverClass);
	// }
	// catch (ClassNotFoundException e)
	// {
	// throw new RuntimeException(e);
	// }
	//
	// Connection connection = DriverManager.getConnection(dbJdbcUri, dbUser, dbPassword);
	// try
	// {
	// Statement statement = connection.createStatement();
	// try
	// {
	// statement.executeUpdate("DROP DATABASE " + dbName + ";");
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
	// LOG.debug("Destroyed database [{}]", dbName);
	// }
	// }

	@Bean
	public static PropertySourcesPlaceholderConfigurer properties()
	{
		PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
		Resource[] resources = new Resource[]
		{ new FileSystemResource(System.getProperty("molgenis.home") + "/molgenis-server.properties"),
				new ClassPathResource("/molgenis.properties") };
		pspc.setLocations(resources);
		pspc.setFileEncoding("UTF-8");
		pspc.setIgnoreUnresolvablePlaceholders(true);
		pspc.setIgnoreResourceNotFound(true);
		pspc.setNullValue("@null");
		return pspc;
	}

	@Bean
	public FreeMarkerConfigurer freeMarkerConfigurer()
	{
		return new FreeMarkerConfigurer();
	}

	@Bean
	public ConversionService conversionService()
	{
		return new DefaultConversionService();
	}

	@Bean
	public ApplicationContextProvider applicationContextProvider()
	{
		return new ApplicationContextProvider();
	}

	@Bean
	public PasswordEncoder passwordEncoder()
	{
		return new MolgenisPasswordEncoder(new BCryptPasswordEncoder());
	}

}
