package org.molgenis.ui;

import org.molgenis.auth.MolgenisUserDecorator;
import org.molgenis.auth.MolgenisUserMetaData;
import org.molgenis.data.AutoValueRepositoryDecorator;
import org.molgenis.data.ComputedEntityValuesDecorator;
import org.molgenis.data.DataService;
import org.molgenis.data.EntityManager;
import org.molgenis.data.EntityReferenceResolverDecorator;
import org.molgenis.data.IdGenerator;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryDecoratorFactory;
import org.molgenis.data.RepositorySecurityDecorator;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.molgenis.data.cache.TransactionEntityCache;
import org.molgenis.data.cache.TransactionEntityCacheDecorator;
import org.molgenis.data.i18n.I18nStringDecorator;
import org.molgenis.data.i18n.I18nStringMetaData;
import org.molgenis.data.i18n.LanguageMetaData;
import org.molgenis.data.i18n.LanguageRepositoryDecorator;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.meta.AttributeMetaDataMetaData;
import org.molgenis.data.meta.AttributeMetaDataRepositoryDecorator;
import org.molgenis.data.meta.EntityMetaDataMetaData;
import org.molgenis.data.meta.EntityMetaDataRepositoryDecorator;
import org.molgenis.data.meta.MetaDataRepositoryDecorator;
import org.molgenis.data.meta.PackageMetaData;
import org.molgenis.data.meta.PackageRepositoryDecorator;
import org.molgenis.data.meta.TagMetaData;
import org.molgenis.data.mysql.MysqlRepositoryCollection;
import org.molgenis.data.settings.AppSettings;
import org.molgenis.data.support.OwnedEntityMetaData;
import org.molgenis.data.transaction.TransactionLogRepositoryDecorator;
import org.molgenis.data.transaction.TransactionLogService;
import org.molgenis.data.validation.EntityAttributesValidator;
import org.molgenis.data.validation.ExpressionValidator;
import org.molgenis.security.owned.OwnedEntityRepositoryDecorator;
import org.molgenis.util.EntityUtils;
import org.molgenis.util.MySqlRepositoryExceptionTranslatorDecorator;

public class MolgenisRepositoryDecoratorFactory implements RepositoryDecoratorFactory
{
	private final EntityManager entityManager;
	private final TransactionLogService transactionLogService;
	private final EntityAttributesValidator entityAttributesValidator;
	private final IdGenerator idGenerator;
	private final AppSettings appSettings;
	private final DataService dataService;
	private final ExpressionValidator expressionValidator;
	private final RepositoryDecoratorRegistry repositoryDecoratorRegistry;
	private final LanguageService languageService;
	private final SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;
	private final TransactionEntityCache transactionEntityCache;

	public MolgenisRepositoryDecoratorFactory(EntityManager entityManager, TransactionLogService transactionLogService,
			EntityAttributesValidator entityAttributesValidator, IdGenerator idGenerator, AppSettings appSettings,
			DataService dataService, ExpressionValidator expressionValidator,
			RepositoryDecoratorRegistry repositoryDecoratorRegistry, LanguageService languageService,
			SystemEntityMetaDataRegistry systemEntityMetaDataRegistry, TransactionEntityCache transactionEntityCache)
	{
		this.entityManager = entityManager;
		this.transactionLogService = transactionLogService;
		this.entityAttributesValidator = entityAttributesValidator;
		this.idGenerator = idGenerator;
		this.appSettings = appSettings;
		this.dataService = dataService;
		this.expressionValidator = expressionValidator;
		this.repositoryDecoratorRegistry = repositoryDecoratorRegistry;
		this.languageService = languageService;
		this.systemEntityMetaDataRegistry = systemEntityMetaDataRegistry;
		this.transactionEntityCache = transactionEntityCache;
	}

	@Override
	public Repository createDecoratedRepository(Repository repository)
	{
		Repository decoratedRepository = repositoryDecoratorRegistry.decorate(repository);

		// 10. Custom decorators
		decoratedRepository = applyCustomRepositoryDecorators(decoratedRepository);

		// 9. Owned decorator
		if (EntityUtils.doesExtend(decoratedRepository.getEntityMetaData(), OwnedEntityMetaData.ENTITY_NAME))
		{
			decoratedRepository = new OwnedEntityRepositoryDecorator(decoratedRepository);
		}

		// 8. Entity reference resolver decorator
		decoratedRepository = new EntityReferenceResolverDecorator(decoratedRepository, entityManager);

		// 7. Computed entity values decorator
		decoratedRepository = new ComputedEntityValuesDecorator(decoratedRepository);

		// 6. Entity listener
		decoratedRepository = new EntityListenerRepositoryDecorator(decoratedRepository);

		// 5. Transaction log decorator
		decoratedRepository = new TransactionLogRepositoryDecorator(decoratedRepository, transactionLogService);

		// 4. SQL exception translation decorator
		String backend = decoratedRepository.getEntityMetaData().getBackend();
		if (MysqlRepositoryCollection.NAME.equals(backend))
		{
			decoratedRepository = new MySqlRepositoryExceptionTranslatorDecorator(decoratedRepository);
		}

		// FIXME enable validation
		// 3. validation decorator
		// decoratedRepository = new RepositoryValidationDecorator(dataService, decoratedRepository,
		// entityAttributesValidator, expressionValidator);

		// 2. auto value decorator
		decoratedRepository = new AutoValueRepositoryDecorator(decoratedRepository, idGenerator);

		// 1. security decorator
		decoratedRepository = new RepositorySecurityDecorator(decoratedRepository, appSettings);

		// 0. meta data security decorator
		if (dataService.getMeta().isMetaRepository(repository.getName()))
		{
			decoratedRepository = new MetaDataRepositoryDecorator(decoratedRepository, dataService.getMeta());
		}

		// -1. transaction cache decorator
		decoratedRepository = new TransactionEntityCacheDecorator(decoratedRepository, transactionEntityCache);

		return decoratedRepository;
	}

	/**
	 * Apply custom repository decorators based on entity meta data
	 * 
	 * @param repository
	 * @return
	 */
	private Repository applyCustomRepositoryDecorators(Repository repository)
	{
		if (repository.getName().equals(MolgenisUserMetaData.ENTITY_NAME))
		{
			repository = new MolgenisUserDecorator(repository);
		}
		else if (repository.getName().equals(AttributeMetaDataMetaData.ENTITY_NAME))
		{
			repository = new AttributeMetaDataRepositoryDecorator(repository, systemEntityMetaDataRegistry);
		}
		else if (repository.getName().equals(EntityMetaDataMetaData.ENTITY_NAME))
		{
			repository = new EntityMetaDataRepositoryDecorator(repository, dataService, systemEntityMetaDataRegistry,
					languageService);
		}
		else if (repository.getName().equals(PackageMetaData.ENTITY_NAME))
		{
			repository = new PackageRepositoryDecorator(repository, dataService, systemEntityMetaDataRegistry);
		}
		else if (repository.getName().equals(LanguageMetaData.ENTITY_NAME))
		{
			repository = new LanguageRepositoryDecorator(repository, dataService);
		}
		else if (repository.getName().equals(I18nStringMetaData.ENTITY_NAME))
		{
			repository = new I18nStringDecorator(repository);
		}
		else if (repository.getName().equals(TagMetaData.ENTITY_NAME))
		{
			// do nothing
		}
		return repository;
	}
}
