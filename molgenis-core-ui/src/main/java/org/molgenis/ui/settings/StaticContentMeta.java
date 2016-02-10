package org.molgenis.ui.settings;

import static org.molgenis.MolgenisFieldTypes.TEXT;
import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class StaticContentMeta extends SystemEntityMetaData
{
	public StaticContentMeta()
	{
		super(StaticContent.ENTITY_NAME, StaticContent.class);

		addAttribute(StaticContent.KEY, ROLE_ID).setLabel("Key");
		addAttribute(StaticContent.CONTENT).setDataType(TEXT).setLabel("Content");
	}
}