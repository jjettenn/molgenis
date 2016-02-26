package org.molgenis;

import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.Test;

public class MolgenisFieldTypesTest
{
	@Test
	public void getTypeNames()
	{
		List<String> typeNames = MolgenisFieldTypes.getTypeNames();
		assertTrue(typeNames.contains("datetime"));
		assertTrue(typeNames.contains("string"));
	}
}
