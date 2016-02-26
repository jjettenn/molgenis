package org.molgenis.data.mapper.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.molgenis.auth.MolgenisUser;
import org.molgenis.data.DataService;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.mapper.mapping.model.MappingProject;
import org.molgenis.data.mapper.mapping.model.MappingTarget;
import org.molgenis.data.mapper.repository.MappingProjectRepository;
import org.molgenis.data.mapper.service.AlgorithmService;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.security.permission.PermissionSystemService;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MappingServiceImplTest
{
	private MappingServiceImpl mappingServiceImpl;
	private MetaDataService metaDataService;
	private DataService dataService;
	private AlgorithmService algorithmService;
	private MappingProjectRepository mappingProjectRepository;
	private PermissionSystemService permissionSystemService;
	private SecurityContext securityContext;

	@BeforeMethod
	public void setUpBeforeMethod()
	{
		metaDataService = mock(MetaDataService.class);
		dataService = mock(DataService.class);
		when(dataService.getMeta()).thenReturn(metaDataService);
		algorithmService = mock(AlgorithmService.class);
		mappingProjectRepository = mock(MappingProjectRepository.class);
		permissionSystemService = mock(PermissionSystemService.class);
		mappingServiceImpl = new MappingServiceImpl(dataService, algorithmService, mappingProjectRepository,
				permissionSystemService);

		securityContext = mock(SecurityContext.class);
		SecurityContextHolder.setContext(securityContext);
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void MappingServiceImpl()
	{
		new MappingServiceImpl(null, null, null, null);
	}

	@Test
	public void addMappingProject()
	{
		String projectName = "project";
		MolgenisUser owner = mock(MolgenisUser.class);
		String target = "targetEntity";
		EntityMetaData targetEntityMeta = mock(EntityMetaData.class);
		when(dataService.getEntityMetaData(target)).thenReturn(targetEntityMeta);
		MappingProject mappingProject = mappingServiceImpl.addMappingProject(projectName, owner, target);
		verify(mappingProjectRepository, times(1)).add(mappingProject);
		assertEquals(mappingProject.getName(), projectName);
		assertEquals(mappingProject.getOwner(), owner);
		List<MappingTarget> mappingTargets = mappingProject.getMappingTargets();
		assertEquals(mappingTargets.size(), 1);
		assertEquals(mappingTargets.get(0).getTarget(), targetEntityMeta);
	}

	// @Test
	// public void applyMappingsTargetDoesNotExist()
	// {
	// String targetEntityName = "targetEntity0";
	// EntityMetaData targetEntityMeta = mock(EntityMetaData.class);
	// when(targetEntityMeta.getName()).thenReturn(targetEntityName);
	// when(targetEntityMeta.getSimpleName()).thenReturn(targetEntityName);
	// when(targetEntityMeta.getAttributes()).thenReturn(emptyList());
	// when(targetEntityMeta.getOwnAttributes()).thenReturn(emptyList());
	// when(targetEntityMeta.getOwnLookupAttributes()).thenReturn(emptyList());
	// when(targetEntityMeta.getOwnTags()).thenReturn(emptyList());
	// AttributeMetaData targetIdAttr = mock(AttributeMetaData.class);
	// when(targetIdAttr.getName()).thenReturn("targetId");
	// when(targetEntityMeta.getIdAttribute()).thenReturn(targetIdAttr);
	// String entityName = "entity0";
	// EntityMetaData targetEntityMetaWithNewName = mock(EntityMetaData.class);
	// when(targetEntityMetaWithNewName.getName()).thenReturn(entityName);
	// when(targetEntityMetaWithNewName.getSimpleName()).thenReturn(entityName);
	// when(targetEntityMetaWithNewName.getOwnAttributes()).thenReturn(emptyList());
	// when(targetEntityMetaWithNewName.getOwnLookupAttributes()).thenReturn(emptyList());
	// when(targetEntityMetaWithNewName.getOwnTags()).thenReturn(emptyList());
	//
	// MappingTarget mappingTarget = mock(MappingTarget.class);
	// when(mappingTarget.getTarget()).thenReturn(targetEntityMeta);
	//
	// EntityMapping sourceEntityMapping0 = mock(EntityMapping.class);
	// String sourceEntityName0 = "sourceEntity0";
	// when(sourceEntityMapping0.getName()).thenReturn(sourceEntityName0);
	// AttributeMapping sourceAttr0Mappings0 = mock(AttributeMapping.class);
	// AttributeMetaData targetAttr0 = mock(AttributeMetaData.class);
	// when(targetAttr0.getName()).thenReturn("targetAttr0");
	// when(sourceAttr0Mappings0.getTargetAttributeMetaData()).thenReturn(targetAttr0);
	// AttributeMapping sourceAttr1Mappings0 = mock(AttributeMapping.class);
	// AttributeMetaData targetAttr1 = mock(AttributeMetaData.class);
	// when(targetAttr1.getName()).thenReturn("targetAttr1");
	// when(sourceAttr1Mappings0.getTargetAttributeMetaData()).thenReturn(targetAttr1);
	// List<AttributeMapping> sourceAttrMappings0 = Arrays.asList(sourceAttr0Mappings0, sourceAttr1Mappings0);
	// when(sourceEntityMapping0.getAttributeMappings()).thenReturn(sourceAttrMappings0);
	// EntityMetaData sourceEntityMeta0 = mock(EntityMetaData.class);
	// when(sourceEntityMapping0.getSourceEntityMetaData()).thenReturn(sourceEntityMeta0);
	//
	// EntityMapping sourceEntityMapping1 = mock(EntityMapping.class);
	// String sourceEntityName1 = "sourceEntity1";
	// when(sourceEntityMapping1.getName()).thenReturn(sourceEntityName1);
	// AttributeMapping sourceAttr0Mappings1 = mock(AttributeMapping.class);
	// when(sourceAttr0Mappings1.getTargetAttributeMetaData()).thenReturn(targetAttr0);
	// AttributeMapping sourceAttr1Mappings1 = mock(AttributeMapping.class);
	// when(sourceAttr1Mappings1.getTargetAttributeMetaData()).thenReturn(targetAttr1);
	// List<AttributeMapping> sourceAttrMappings1 = Arrays.asList(sourceAttr0Mappings1, sourceAttr1Mappings1);
	// when(sourceEntityMapping1.getAttributeMappings()).thenReturn(sourceAttrMappings1);
	// EntityMetaData sourceEntityMeta1 = mock(EntityMetaData.class);
	// when(sourceEntityMapping1.getSourceEntityMetaData()).thenReturn(sourceEntityMeta1);
	//
	// List<EntityMapping> entityMappings = Arrays.asList(sourceEntityMapping0, sourceEntityMapping1);
	//
	// when(mappingTarget.getEntityMappings()).thenReturn(entityMappings);
	// Repository sourceRepo0 = mock(Repository.class);
	// Entity sourceRepo0Entity0 = mock(Entity.class);
	// Entity sourceRepo0Entity1 = mock(Entity.class);
	// List<Entity> sourceRepo0Entities = Arrays.asList(sourceRepo0Entity0, sourceRepo0Entity1);
	// when(sourceRepo0.iterator()).thenReturn(sourceRepo0Entities.iterator());
	// when(dataService.getRepository(sourceEntityName0)).thenReturn(sourceRepo0);
	// Repository sourceRepo1 = mock(Repository.class);
	// Entity sourceRepo1Entity0 = mock(Entity.class);
	// Entity sourceRepo1Entity1 = mock(Entity.class);
	// List<Entity> sourceRepo1Entities = Arrays.asList(sourceRepo1Entity0, sourceRepo1Entity1);
	// when(sourceRepo1.iterator()).thenReturn(sourceRepo1Entities.iterator());
	// when(dataService.getRepository(sourceEntityName1)).thenReturn(sourceRepo1);
	// Repository targetRepo = mock(Repository.class);
	// when(targetRepo.getName()).thenReturn(targetEntityName);
	// when(targetRepo.getEntityMetaData()).thenReturn(targetEntityMeta);
	// when(targetRepo.findAll(any(Query.class))).thenAnswer(new Answer<Stream<Entity>>()
	// {
	// @Override
	// public Stream<Entity> answer(InvocationOnMock invocation) throws Throwable
	// {
	// return Collections.<Entity> emptyList().stream();
	// }
	// });
	//
	// when(metaDataService.addEntityMeta(argThat(eqName(targetEntityMetaWithNewName)))).thenReturn(targetRepo);
	//
	// when(algorithmService.apply(sourceAttr0Mappings0, sourceRepo0Entity0, sourceEntityMeta0))
	// .thenReturn("source0-entity0-attr0");
	// when(algorithmService.apply(sourceAttr0Mappings1, sourceRepo0Entity0, sourceEntityMeta0))
	// .thenReturn("source0-entity0-attr1");
	// when(algorithmService.apply(sourceAttr0Mappings0, sourceRepo0Entity1, sourceEntityMeta0))
	// .thenReturn("source0-entity1-attr0");
	// when(algorithmService.apply(sourceAttr0Mappings1, sourceRepo0Entity1, sourceEntityMeta0))
	// .thenReturn("source0-entity1-attr1");
	// when(algorithmService.apply(sourceAttr0Mappings0, sourceRepo1Entity0, sourceEntityMeta1))
	// .thenReturn("source1-entity0-attr0");
	// when(algorithmService.apply(sourceAttr0Mappings1, sourceRepo1Entity0, sourceEntityMeta1))
	// .thenReturn("source1-entity0-attr1");
	// when(algorithmService.apply(sourceAttr0Mappings0, sourceRepo1Entity1, sourceEntityMeta1))
	// .thenReturn("source1-entity1-attr0");
	// when(algorithmService.apply(sourceAttr0Mappings1, sourceRepo1Entity1, sourceEntityMeta1))
	// .thenReturn("source1-entity1-attr1");
	//
	// assertEquals(entityName, mappingServiceImpl.applyMappings(mappingTarget, entityName));
	//
	// verify(metaDataService, times(1)).addEntityMeta(argThat(eqName(targetEntityMetaWithNewName)));
	// verify(permissionSystemService, times(1)).giveUserEntityPermissions(securityContext,
	// singletonList(targetEntityName));
	//
	// @SuppressWarnings(
	// { "unchecked", "rawtypes" })
	// ArgumentCaptor<Stream<Entity>> captor = ArgumentCaptor.forClass((Class) Stream.class);
	// verify(targetRepo, times(2)).add(captor.capture());
	// List<Stream<Entity>> allValues = captor.getAllValues();
	//
	// System.out.println(allValues.get(0).collect(toList()));
	// System.out.println(allValues.get(1).collect(toList()));
	// }
	//
	// @Test
	// public void applyMappingsExistingTargetCompatible()
	// {
	// throw new RuntimeException();
	// }
	//
	// @Test
	// public void applyMappingsExistingTargetNotCompatible()
	// {
	// throw new RuntimeException();
	// }
	//
	// @Test
	// public void cloneMappingProjectString()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void cloneMappingProjectStringString()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void cloneMappingProjectMappingProjectString()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void deleteMappingProject()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void generateId()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void getAllMappingProjects()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void getMappingProject()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void isTargetMetaCompatible()
	// {
	// throw new RuntimeException("Test not implemented");
	// }
	//
	// @Test
	// public void updateMappingProject()
	// {
	// throw new RuntimeException("Test not implemented");
	// }

	private static Matcher<EntityMetaData> eqName(EntityMetaData expectedEntityMeta)
	{
		return new BaseMatcher<EntityMetaData>()
		{
			@Override
			public boolean matches(Object item)
			{
				if (!(item instanceof EntityMetaData))
				{
					return false;
				}
				return ((EntityMetaData) item).getName().equals(expectedEntityMeta.getName());
			}

			@Override
			public void describeTo(Description description)
			{
				description.appendText("is EntityMetaData with same name");
			}
		};
	}
}
