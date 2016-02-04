package org.molgenis.data.meta;

import static java.util.Objects.requireNonNull;

import org.molgenis.data.DataService;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryDecoratorFactory;
import org.molgenis.data.i18n.I18nStringDecorator;
import org.molgenis.data.i18n.I18nStringMetaData;
import org.molgenis.data.i18n.LanguageMetaData;
import org.molgenis.data.i18n.LanguageRepositoryDecorator;

public class MetaDataRepositoryDecoratorFactory implements RepositoryDecoratorFactory
{
	private final DataService dataService;
	private final MetaDataService metaDataService;

	public MetaDataRepositoryDecoratorFactory(DataService dataService, MetaDataService metaDataService)
	{
		this.dataService = requireNonNull(dataService);
		this.metaDataService = requireNonNull(metaDataService);
	}

	@Override
	public Repository createDecoratedRepository(Repository repository)
	{
		if (repository.getName().equals(AttributeMetaDataMetaData.INSTANCE.getName()))
		{
			repository = new AttributeMetaDataRepositoryDecorator(repository);
		}
		else if (repository.getName().equals(EntityMetaDataMetaData.INSTANCE.getName()))
		{
			// do nothing
		}
		else if (repository.getName().equals(PackageMetaData.INSTANCE.getName()))
		{
			// do nothing
		}
		else if (repository.getName().equals(LanguageMetaData.INSTANCE.getName()))
		{
			repository = new LanguageRepositoryDecorator(repository, dataService);
		}
		else if (repository.getName().equals(I18nStringMetaData.INSTANCE.getName()))
		{
			repository = new I18nStringDecorator(repository);
		}
		else if (repository.getName().equals(TagMetaData.INSTANCE.getName()))
		{
			// do nothing
		}

		repository = new MetaDataRepositoryDecorator(repository, metaDataService);

		return repository;
	}

}
