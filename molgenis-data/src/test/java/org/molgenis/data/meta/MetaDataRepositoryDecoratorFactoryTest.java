package org.molgenis.data.meta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.molgenis.data.DataService;
import org.molgenis.data.Repository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetaDataRepositoryDecoratorFactoryTest
{
	private MetaDataService metaDataService;
	private DataService dataService;
	private MetaDataRepositoryDecoratorFactory metaDataRepositoryDecoratorFactory;

	@BeforeMethod
	public void setUpBeforeMethod()
	{
		dataService = mock(DataService.class);
		metaDataService = mock(MetaDataService.class);
		metaDataRepositoryDecoratorFactory = new MetaDataRepositoryDecoratorFactory(dataService, metaDataService);
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void MetaDataRepositoryDecoratorFactory()
	{
		new MetaDataRepositoryDecoratorFactory(null, null);
	}

	@Test
	public void createDecoratedRepository()
	{
		Repository repo = mock(Repository.class);
		when(repo.getName()).thenReturn("repo");
		Repository decoratedRepo = metaDataRepositoryDecoratorFactory.createDecoratedRepository(repo);
		assertEquals(decoratedRepo.getClass(), MetaDataRepositoryDecorator.class);
	}
}
