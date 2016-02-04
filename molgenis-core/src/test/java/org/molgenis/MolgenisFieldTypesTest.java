package org.molgenis;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.molgenis.MolgenisFieldTypes.FieldTypeEnum;
import org.testng.annotations.Test;

public class MolgenisFieldTypesTest
{
	@Test
	public void fieldTypeEnumGet()
	{
		assertEquals(FieldTypeEnum.get("datetime"), FieldTypeEnum.DATE_TIME);
		assertEquals(FieldTypeEnum.get("date_time"), FieldTypeEnum.DATE_TIME);
		assertEquals(FieldTypeEnum.get("DATETIME"), FieldTypeEnum.DATE_TIME);
		assertEquals(FieldTypeEnum.get("DATE_TIME"), FieldTypeEnum.DATE_TIME);
	}

	@Test
	public void fieldTypeEnumGetOptionsLowercase()
	{
		assertTrue(FieldTypeEnum.getOptionsLowercase().contains("datetime"));
	}
}
