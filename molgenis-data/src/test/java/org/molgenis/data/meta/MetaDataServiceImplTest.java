package org.molgenis.data.meta;

import org.testng.annotations.Test;

public class MetaDataServiceImplTest
{
	@Test(expectedExceptions = NullPointerException.class)
	public void MetaDataServiceImpl()
	{
		new MetaDataServiceImpl(null);
	}

	// FIXME add tests
}
