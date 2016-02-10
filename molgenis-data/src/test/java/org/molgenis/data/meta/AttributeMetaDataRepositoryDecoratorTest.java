package org.molgenis.data.meta;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.molgenis.data.DataService;
import org.molgenis.data.Entity;
import org.molgenis.data.MolgenisDataException;
import org.molgenis.data.Repository;
import org.molgenis.data.SystemEntityMetaDataRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AttributeMetaDataRepositoryDecoratorTest
{
	private Repository decoratedRepo;
	private DataService dataService;
	private SystemEntityMetaDataRegistry systemEntityMetaDataRegistry;
	private AttributeMetaDataRepositoryDecorator attributeMetaDataRepositoryDecorator;

	@BeforeMethod
	public void setUpBeforeMethod()
	{
		decoratedRepo = mock(Repository.class);
		dataService = mock(DataService.class);
		systemEntityMetaDataRegistry = mock(SystemEntityMetaDataRegistry.class);
		attributeMetaDataRepositoryDecorator = new AttributeMetaDataRepositoryDecorator(decoratedRepo,
				systemEntityMetaDataRegistry);
	}

	@SuppressWarnings("resource")
	@Test(expectedExceptions = NullPointerException.class)
	public void AttributeMetaDataRepositoryDecorator()
	{
		new AttributeMetaDataRepositoryDecorator(null, null);
	}

	@Test
	public void updateDataTypeAllowed()
	{
		String entityId = "id0";

		Entity currentEntity = mock(Entity.class);
		when(currentEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(currentEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("categorical");
		when(decoratedRepo.findOne(entityId)).thenReturn(currentEntity);

		Entity updatedEntity = mock(Entity.class);
		when(updatedEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(updatedEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("xref");

		attributeMetaDataRepositoryDecorator.update(updatedEntity);
		verify(decoratedRepo, times(1)).update(updatedEntity);
	}

	@Test(expectedExceptions = MolgenisDataException.class)
	public void updateDataTypeNotAllowed()
	{
		String entityId = "id0";

		Entity currentEntity = mock(Entity.class);
		when(currentEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(currentEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("xref");
		when(decoratedRepo.findOne(entityId)).thenReturn(currentEntity);

		Entity updatedEntity = mock(Entity.class);
		when(updatedEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(updatedEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("mref");

		attributeMetaDataRepositoryDecorator.update(updatedEntity);
	}

	@SuppressWarnings(
	{ "unchecked", "rawtypes" })
	@Test
	public void updateStreamDataTypeAllowed()
	{
		String entityId = "id0";

		Entity currentEntity = mock(Entity.class);
		when(currentEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(currentEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("email");
		when(decoratedRepo.findOne(entityId)).thenReturn(currentEntity);

		Entity updatedEntity = mock(Entity.class);
		when(updatedEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(updatedEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("string");

		attributeMetaDataRepositoryDecorator.update(Stream.of(updatedEntity));
		ArgumentCaptor<Stream<Entity>> captor = ArgumentCaptor.forClass((Class) Stream.class);
		verify(decoratedRepo, times(1)).update(captor.capture());
		List<Entity> actualList = captor.getValue().collect(toList());
		assertEquals(actualList, Arrays.asList(updatedEntity));
	}

	@SuppressWarnings(
	{ "unchecked", "rawtypes" })
	@Test(expectedExceptions = MolgenisDataException.class)
	public void updateStreamDataTypeNotAllowed()
	{
		String entityId = "id0";

		Entity currentEntity = mock(Entity.class);
		when(currentEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(currentEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("bool");
		when(decoratedRepo.findOne(entityId)).thenReturn(currentEntity);

		Entity updatedEntity = mock(Entity.class);
		when(updatedEntity.getString(AttributeMetaDataMetaData.IDENTIFIER)).thenReturn(entityId);
		when(updatedEntity.getString(AttributeMetaDataMetaData.DATA_TYPE)).thenReturn("hyperlink");

		attributeMetaDataRepositoryDecorator.update(Stream.of(updatedEntity));
		ArgumentCaptor<Stream<Entity>> captor = ArgumentCaptor.forClass((Class) Stream.class);
		verify(decoratedRepo, times(1)).update(captor.capture());
		captor.getValue().collect(toList());
	}
}
