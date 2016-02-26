package org.molgenis.data.meta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.molgenis.data.RepositoryCapability.INDEXABLE;
import static org.molgenis.data.RepositoryCapability.MANAGABLE;
import static org.molgenis.data.RepositoryCapability.QUERYABLE;
import static org.testng.Assert.assertEquals;

import java.util.stream.Stream;

import org.molgenis.data.Entity;
import org.molgenis.data.Fetch;
import org.molgenis.data.Repository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class MetaDataRepositoryDecoratorTest
{
	private MetaDataService metaDataService;
	private Repository decoratedRepo;
	private MetaDataRepositoryDecorator metaDataRepoDecorator;

	@BeforeMethod
	public void setUpBeforeMethod()
	{
		decoratedRepo = mock(Repository.class);
		metaDataService = mock(MetaDataService.class);
		metaDataRepoDecorator = new MetaDataRepositoryDecorator(decoratedRepo, metaDataService);
	}

	@Test
	public void getCapabilities()
	{
		when(decoratedRepo.getCapabilities()).thenReturn(Sets.newHashSet(INDEXABLE, QUERYABLE, MANAGABLE));
		assertEquals(metaDataRepoDecorator.getCapabilities(), Sets.newHashSet(INDEXABLE, QUERYABLE));
	}

	@Test
	public void addEntity()
	{
		Entity entity = mock(Entity.class);
		metaDataRepoDecorator.add(entity);
		verify(decoratedRepo, times(1)).add(entity);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void addStream()
	{
		Entity entity = mock(Entity.class);
		Stream<Entity> stream = Stream.of(entity);
		metaDataRepoDecorator.add(stream);
		verify(decoratedRepo, times(1)).add(stream);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void deleteEntity()
	{
		Entity entity = mock(Entity.class);
		metaDataRepoDecorator.delete(entity);
		verify(decoratedRepo, times(1)).delete(entity);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void deleteStream()
	{
		Entity entity = mock(Entity.class);
		Stream<Entity> stream = Stream.of(entity);
		metaDataRepoDecorator.delete(stream);
		verify(decoratedRepo, times(1)).delete(stream);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void deleteAll()
	{
		metaDataRepoDecorator.deleteAll();
		verify(decoratedRepo, times(1)).deleteAll();
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void deleteByIdObject()
	{
		Object id = mock(Object.class);
		metaDataRepoDecorator.deleteById(id);
		verify(decoratedRepo, times(1)).deleteById(id);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void deleteByIdStream()
	{
		Object id = mock(Object.class);
		Stream<Object> stream = Stream.of(id);
		metaDataRepoDecorator.deleteById(stream);
		verify(decoratedRepo, times(1)).deleteById(stream);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void updateEntity()
	{
		Entity entity = mock(Entity.class);
		metaDataRepoDecorator.update(entity);
		verify(decoratedRepo, times(1)).update(entity);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void updateStream()
	{
		Entity entity = mock(Entity.class);
		Stream<Entity> stream = Stream.of(entity);
		metaDataRepoDecorator.update(stream);
		verify(decoratedRepo, times(1)).update(stream);
		verify(metaDataService, times(1)).refreshCaches();
	}

	@Test
	public void streamFetch()
	{
		Fetch fetch = new Fetch();
		metaDataRepoDecorator.stream(fetch);
		verify(decoratedRepo, times(1)).stream(fetch);
	}
}
