package org.molgenis.app;

import java.io.IOException;
import java.util.Map;

import org.molgenis.CommandLineOnlyConfiguration;
import org.molgenis.DatabaseConfig;
import org.molgenis.data.DataService;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.config.HttpClientConfig;
import org.molgenis.data.elasticsearch.ElasticsearchRepositoryCollection;
import org.molgenis.data.elasticsearch.config.EmbeddedElasticSearchConfig;
import org.molgenis.data.elasticsearch.factory.EmbeddedElasticSearchServiceFactory;
import org.molgenis.data.mysql.MysqlRepositoryCollection;
import org.molgenis.data.system.RepositoryTemplateLoader;
import org.molgenis.dataexplorer.freemarker.DataExplorerHyperlinkDirective;
import org.molgenis.migrate.version.v1_11.Step20RebuildElasticsearchIndex;
import org.molgenis.migrate.version.v1_11.Step21SetLoggingEventBackend;
import org.molgenis.migrate.version.v1_13.Step22RemoveDiseaseMatcher;
import org.molgenis.migrate.version.v1_14.Step23RebuildElasticsearchIndex;
import org.molgenis.migrate.version.v1_15.Step24UpdateApplicationSettings;
import org.molgenis.migrate.version.v1_15.Step25LanguagesPermissions;
import org.molgenis.migrate.version.v1_16.Step26migrateJpaBackend;
import org.molgenis.migrate.version.v1_17.Step27MetaDataAttributeRoles;
import org.molgenis.ui.MolgenisWebAppConfig;
import org.molgenis.util.GsonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import com.google.gson.Gson;

import freemarker.template.TemplateException;

@Configuration
@EnableTransactionManagement
@EnableWebMvc
@EnableAsync
@ComponentScan(basePackages = "org.molgenis", excludeFilters = @Filter(type = FilterType.ANNOTATION, value = CommandLineOnlyConfiguration.class))
@Import(
{ WebAppSecurityConfig.class, DatabaseConfig.class, HttpClientConfig.class, EmbeddedElasticSearchConfig.class,
		GsonConfig.class })
public class WebAppConfig extends MolgenisWebAppConfig
{
	private static final Logger LOG = LoggerFactory.getLogger(WebAppConfig.class);

	@Autowired
	private DataService dataService;

	@Autowired
	@Qualifier("MysqlRepositoryCollection")
	private RepositoryCollection mysqlRepositoryCollection;

	@Autowired
	private ElasticsearchRepositoryCollection elasticsearchRepositoryCollection;

	@Autowired
	private EmbeddedElasticSearchServiceFactory embeddedElasticSearchServiceFactory;

	@Autowired
	private Gson gson;

	@Autowired
	private Step20RebuildElasticsearchIndex step20RebuildElasticsearchIndex;

	@Autowired
	private Step23RebuildElasticsearchIndex step23RebuildElasticsearchIndex;

	@Override
	public RepositoryCollection getBackend()
	{
		return mysqlRepositoryCollection;
	}

	@Override
	public void addUpgrades()
	{
		upgradeService.addUpgrade(step20RebuildElasticsearchIndex);
		upgradeService.addUpgrade(new Step21SetLoggingEventBackend(dataSource));
		upgradeService.addUpgrade(new Step22RemoveDiseaseMatcher(dataSource));
		upgradeService.addUpgrade(step23RebuildElasticsearchIndex);
		upgradeService.addUpgrade(new Step24UpdateApplicationSettings(dataSource, idGenerator));
		upgradeService.addUpgrade(new Step25LanguagesPermissions(dataService));
		upgradeService.addUpgrade(new Step26migrateJpaBackend(dataSource, MysqlRepositoryCollection.NAME, idGenerator));
		upgradeService.addUpgrade(new Step27MetaDataAttributeRoles(dataSource));
	}

	@Override
	protected void addFreemarkerVariables(Map<String, Object> freemarkerVariables)
	{
		freemarkerVariables.put("dataExplorerLink",
				new DataExplorerHyperlinkDirective(molgenisPluginRegistry(), dataService));
	}

	@Override
	public FreeMarkerConfigurer freeMarkerConfigurer() throws IOException, TemplateException
	{
		FreeMarkerConfigurer result = super.freeMarkerConfigurer();
		// Look up unknown templates in the FreemarkerTemplate repository
		result.setPostTemplateLoaders(new RepositoryTemplateLoader(dataService));
		return result;
	}
}
