package org.molgenis.data.mysql;

import javax.sql.DataSource;

import org.molgenis.data.DataService;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.elasticsearch.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class MySqlConfiguration
{
	@Autowired
	private DataService dataService;

	@Autowired
	private MySqlEntityFactory mySqlEntityFactory;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private SearchService searchService;

	@Autowired
	private SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;

	@Bean
	public AsyncJdbcTemplate asyncJdbcTemplate()
	{
		return new AsyncJdbcTemplate(new JdbcTemplate(dataSource));
	}

	@Bean
	@Scope("prototype")
	public MysqlRepository mysqlRepository()
	{
		return new MysqlRepository(dataService, mySqlEntityFactory, dataSource, asyncJdbcTemplate());
	}

	@Bean(name =
	{ "MysqlRepositoryCollection" })
	public RepositoryCollection mysqlRepositoryCollection()
	{
		MysqlRepositoryCollection mysqlRepositoryCollection = new MysqlRepositoryCollection(dataSource,
				mySqlEntityFactory, systemEntityMetaDataRegistry)
		{
			@Override
			protected MysqlRepository createMysqlRepository()
			{
				return mysqlRepository();
			}

			// @Override
			// public boolean hasRepository(String name)
			// {
			// throw new UnsupportedOperationException();
			// }
		};

		// FIXME this does not belong here
		return mysqlRepositoryCollection;
		// return new IndexedRepositoryCollectionDecorator(searchService, mysqlRepositoryCollection);
	}
}
