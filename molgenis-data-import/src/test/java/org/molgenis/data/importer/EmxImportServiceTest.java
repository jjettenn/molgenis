package org.molgenis.data.importer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.mockito.ArgumentCaptor;
import org.molgenis.data.DataService;
import org.molgenis.data.DatabaseAction;
import org.molgenis.data.EntityMetaData;
import org.molgenis.data.RepositoryCollection;
import org.molgenis.data.meta.MetaDataService;
import org.molgenis.framework.db.EntitiesValidationReport;
import org.molgenis.framework.db.EntityImportReport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EmxImportServiceTest
{
	private EmxImportService emxImportService;
	private MetaDataParser metaDataParser;
	private ImportWriter importWriter;
	private DataService dataService;
	private MetaDataService metaDataService;

	@BeforeMethod
	public void setUpBeforeMethod()
	{
		metaDataParser = mock(MetaDataParser.class);
		importWriter = mock(ImportWriter.class);
		metaDataService = mock(MetaDataService.class);
		dataService = mock(DataService.class);
		when(dataService.getMeta()).thenReturn(metaDataService);
		emxImportService = new EmxImportService(metaDataParser, importWriter, dataService);
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void EmxImportService()
	{
		new EmxImportService(null, null, null);
	}

	@Test
	public void canImportXls()
	{
		RepositoryCollection source = mock(RepositoryCollection.class);
		String entityName = "entity0";
		when(source.getEntityNames()).thenReturn(Arrays.asList(entityName));
		EntityMetaData entityMeta = mock(EntityMetaData.class);
		when(metaDataService.getEntityMetaData(entityName)).thenReturn(entityMeta);
		assertTrue(emxImportService.canImport(new File("file.xls"), source));
	}

	@Test
	public void canImportXlsUnknownEntity()
	{
		RepositoryCollection source = mock(RepositoryCollection.class);
		String entityName = "unknownentity0";
		when(source.getEntityNames()).thenReturn(Arrays.asList(entityName));
		when(metaDataService.getEntityMetaData(entityName)).thenReturn(null);
		assertFalse(emxImportService.canImport(new File("file.xls"), source));
	}

	@Test
	public void canImportXlsx()
	{
		RepositoryCollection source = mock(RepositoryCollection.class);
		String entityName = "entity0";
		when(source.getEntityNames()).thenReturn(Arrays.asList(entityName));
		EntityMetaData entityMeta = mock(EntityMetaData.class);
		when(metaDataService.getEntityMetaData(entityName)).thenReturn(entityMeta);
		assertTrue(emxImportService.canImport(new File("file.xlsx"), source));
	}

	@Test
	public void canImportVcf()
	{
		File file = new File("file.vcf");
		RepositoryCollection source = mock(RepositoryCollection.class);
		assertFalse(emxImportService.canImport(file, source));
	}

	@Test
	public void doImportRepositoryCollectionDatabaseActionString()
	{
		RepositoryCollection source = mock(RepositoryCollection.class);
		DatabaseAction databaseAction = DatabaseAction.ADD;
		String defaultPackage = "package";

		EntityImportReport entityImportReport = mock(EntityImportReport.class);
		when(importWriter.doImport(any(EmxImportJob.class))).thenReturn(entityImportReport);
		assertEquals(emxImportService.doImport(source, databaseAction, defaultPackage), entityImportReport);
		ArgumentCaptor<EmxImportJob> argumentCaptor = ArgumentCaptor.forClass(EmxImportJob.class);
		verify(importWriter, times(1)).doImport(argumentCaptor.capture());
		EmxImportJob emxImportJob = argumentCaptor.getValue();
		assertEquals(emxImportJob.source, source);
		assertEquals(emxImportJob.dbAction, databaseAction);
		assertEquals(emxImportJob.defaultPackage, defaultPackage);
	}

	@Test
	public void validateImport()
	{
		File file = mock(File.class);
		RepositoryCollection source = mock(RepositoryCollection.class);
		EntitiesValidationReport entityValidationReport = mock(EntitiesValidationReport.class);
		when(metaDataParser.validate(source)).thenReturn(entityValidationReport);
		assertEquals(emxImportService.validateImport(file, source), entityValidationReport);
	}
}
