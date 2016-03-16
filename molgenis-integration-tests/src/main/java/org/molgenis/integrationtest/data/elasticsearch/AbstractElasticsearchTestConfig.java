package org.molgenis.integrationtest.data.elasticsearch;

import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.elasticsearch.ElasticsearchRepositoryCollection;
import org.molgenis.integrationtest.data.AbstractDataApiTestConfig;
import org.springframework.context.annotation.Bean;

public abstract class AbstractElasticsearchTestConfig extends AbstractDataApiTestConfig
{
	@Override
	protected RepositoryCollection getBackend()
	{
		return elasticsearchRepositoryCollection();
	}

	@Bean
	public ElasticsearchRepositoryCollection elasticsearchRepositoryCollection()
	{
		return new ElasticsearchRepositoryCollection(searchService, dataService(), null); // FIXME
	}

}
