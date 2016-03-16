package org.molgenis.data.bootstrap;

import static java.util.Objects.requireNonNull;
import static org.molgenis.security.core.runas.RunAsSystemProxy.runAsSystem;

import java.util.List;
import java.util.Map;

import org.molgenis.data.EntityMetaData;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.RepositoryCollectionRegistry;
import org.molgenis.data.i18n.DefaultLanguage;
import org.molgenis.data.meta.DefaultPackage;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.data.support.SystemEntityMetaData;
import org.molgenis.util.DependencyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

/**
 * Bootstraps the MOLGENIS application
 */
@Component
public class MolgenisBootStrapper implements ApplicationListener<ContextRefreshedEvent>, Ordered
{
	private static final Logger LOG = LoggerFactory.getLogger(MolgenisBootStrapper.class);

	private final MetaDataService metaDataService;
	private final RepositoryCollectionRegistry repoCollectionRegistry;

	@Autowired
	public MolgenisBootStrapper(MetaDataService metaDataService, RepositoryCollectionRegistry repoCollectionRegistry,
			DefaultPackage defaultPackage, DefaultLanguage defaultLanguage)
	{
		this.metaDataService = requireNonNull(metaDataService);
		this.repoCollectionRegistry = requireNonNull(repoCollectionRegistry);
	}

	// FIXME make Transactional, not possible because of dependencies on unregistered MolgenisTransactionLog
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event)
	{
		ApplicationContext ctx = event.getApplicationContext();

		LOG.info("Bootstrapping application ...");
		runAsSystem(() -> {
			bootstrapApplication(ctx);
		});
		LOG.info("Bootstrapped application");
	}

	@Override
	public int getOrder()
	{
		return Ordered.HIGHEST_PRECEDENCE;
	}

	private void bootstrapApplication(ApplicationContext ctx)
	{
		// 0. Register repository collections
		Map<String, RepositoryCollection> repoCollectionBeans = ctx.getBeansOfType(RepositoryCollection.class);
		repoCollectionBeans.values().forEach(this::registerRepositoryCollection);

		// 1. Create meta data entity repositories
		repoCollectionRegistry.getRepositoryCollection("MySQL").initMetaDataRepositories(ctx);

		// // 2. Add default package
		// PackageMetaData packageMetaData = ctx.getBean(PackageMetaData.class);
		// metaDataService.upsertEntityMeta(packageMetaData);
		// metaDataService.getRepository(packageMetaData).add(MetaUtils.toEntity(ctx.getBean(DefaultPackage.class)));
		//
		// // 3. Add default language
		// LanguageMetaData languageMetaData = ctx.getBean(LanguageMetaData.class);
		// metaDataService.upsertEntityMeta(languageMetaData);
		// metaDataService.getRepository(languageMetaData).add(ctx.getBean(DefaultLanguage.class));

		Map<String, SystemEntityMetaData> systemEntityMetaDataBeans = ctx.getBeansOfType(SystemEntityMetaData.class);
		List<EntityMetaData> entities = DependencyResolver.resolve(systemEntityMetaDataBeans.values());
		entities.forEach(metaDataService::upsertEntityMeta);
	}

	/**
	 * Registers this repository collection with the repository registry
	 * 
	 * @param repoCollection
	 */
	private void registerRepositoryCollection(RepositoryCollection repoCollection)
	{
		LOG.trace("Registering repository collection [{}] ...", repoCollection.getName());
		repoCollectionRegistry.registerRepositoryCollection(repoCollection);
		LOG.debug("Registered repository collection [{}]", repoCollection.getName());
	}
}
