package org.molgenis.data.meta;

import static org.molgenis.MolgenisFieldTypes.FieldTypeEnum.COMPOUND;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.mockito.Mockito;
import org.molgenis.data.AutoValueRepositoryDecorator;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.IdGenerator;
import org.molgenis.data.ManageableRepositoryCollection;
import org.molgenis.data.Repository;
import org.molgenis.data.RepositoryDecoratorFactory;
import org.molgenis.data.i18n.LanguageService;
import org.molgenis.data.mem.InMemoryRepositoryCollection;
import org.molgenis.data.settings.AppSettings;
import org.molgenis.data.support.DataServiceImpl;
import org.molgenis.data.support.DefaultAttributeMetaData;
import org.molgenis.data.support.DefaultEntityMetaData;
import org.molgenis.data.support.MapEntity;
import org.molgenis.data.support.UuidGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

@WebAppConfiguration
@ContextConfiguration(classes = MetaDataServiceImplTest.Config.class)
public class MetaDataServiceImplTest extends AbstractTestNGSpringContextTests
{
	private MetaDataServiceImpl metaDataServiceImpl;

	@Autowired
	private ManageableRepositoryCollection manageableCrudRepositoryCollection;

	private PackageImpl molgenis;

	@BeforeMethod
	public void readPackageTree()
	{
		PackageImpl org = new PackageImpl("org", "the org package", null);
		molgenis = new PackageImpl("molgenis", "the molgenis package", org);
		org.addSubPackage(molgenis);
		PackageImpl molgenis2 = new PackageImpl("molgenis2", "the molgenis2 package", org);
		org.addSubPackage(molgenis2);

		DefaultEntityMetaData personMetaData = new DefaultEntityMetaData("Person");
		personMetaData.setDescription("A person");
		personMetaData.setPackage(molgenis);
		molgenis.addEntity(personMetaData);

		MapEntity personEntity = new MapEntity();
		personEntity.set(EntityMetaDataMetaData.PACKAGE, molgenis.toEntity());
		personEntity.set(EntityMetaDataMetaData.DESCRIPTION, "A person");
		personEntity.set(EntityMetaDataMetaData.FULL_NAME, "org_molgenis_Person");
		personEntity.set(EntityMetaDataMetaData.SIMPLE_NAME, "Person");
		personEntity.set(EntityMetaDataMetaData.ABSTRACT, true);

		IdGenerator idGenerator = new UuidGenerator();
		DataServiceImpl dataService = new DataServiceImpl(new RepositoryDecoratorFactory()
		{
			@Override
			public Repository createDecoratedRepository(Repository repository)
			{
				return new AutoValueRepositoryDecorator(repository, idGenerator);
			}
		});
		metaDataServiceImpl = new MetaDataServiceImpl(dataService);

		AppSettings appSettings = Mockito.mock(AppSettings.class);
		metaDataServiceImpl.setLanguageService(new LanguageService(dataService, appSettings));
		metaDataServiceImpl.setIdGenerator(new UuidGenerator());
		metaDataServiceImpl.setDefaultBackend(manageableCrudRepositoryCollection);
		metaDataServiceImpl.addPackage(molgenis);
		metaDataServiceImpl.addPackage(molgenis2);
		metaDataServiceImpl.addPackage(org);
		metaDataServiceImpl.addEntityMeta(personMetaData);
	}

	@Test
	public void testAddAndGetEntity()
	{
		readPackageTree();

		PackageImpl defaultPackage = DefaultPackage.INSTANCE;
		DefaultEntityMetaData coderMetaData = new DefaultEntityMetaData("Coder");
		coderMetaData.setDescription("A coder");
		coderMetaData.setExtends(metaDataServiceImpl.getEntityMetaData("org_molgenis_Person"));
		coderMetaData.setPackage(defaultPackage);
		coderMetaData.addAttribute("ID", ROLE_ID);
		coderMetaData.addAttribute("simple");
		DefaultAttributeMetaData compoundAttribute = new DefaultAttributeMetaData("compound", COMPOUND);
		coderMetaData.addAttributeMetaData(compoundAttribute);
		compoundAttribute.addAttributePart(new DefaultAttributeMetaData("c1"));
		compoundAttribute.addAttributePart(new DefaultAttributeMetaData("c2"));

		assertEquals(coderMetaData.getIdAttribute().getName(), "ID");
		metaDataServiceImpl.addEntityMeta(coderMetaData);
		EntityMetaData retrieved = metaDataServiceImpl.getEntityMetaData("Coder");
		assertEquals(retrieved, coderMetaData);
		assertEquals(retrieved.getIdAttribute().getName(), "ID");

		// reboot

		DataServiceImpl dataService = new DataServiceImpl();
		metaDataServiceImpl = new MetaDataServiceImpl(dataService);
		AppSettings appSettings = Mockito.mock(AppSettings.class);
		metaDataServiceImpl.setLanguageService(new LanguageService(dataService, appSettings));
		metaDataServiceImpl.setIdGenerator(new UuidGenerator());
		metaDataServiceImpl.setDefaultBackend(manageableCrudRepositoryCollection);
		retrieved = metaDataServiceImpl.getEntityMetaData("Coder");
		assertEquals(retrieved.getIdAttribute().getName(), "ID");
		assertTrue(Iterables.elementsEqual(retrieved.getAtomicAttributes(), coderMetaData.getAtomicAttributes()));
	}

	@Configuration
	public static class Config
	{
		@Bean
		ManageableRepositoryCollection manageableCrudRepositoryCollection()
		{
			return new InMemoryRepositoryCollection("mem");
		}
	}
}
