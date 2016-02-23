package org.molgenis.data.settings;

import static org.molgenis.data.EntityMetaData.AttributeRole.ROLE_ID;

import org.molgenis.data.support.SystemEntityMetaData;
import org.springframework.stereotype.Component;

@Component
public class SettingsEntityMeta extends SystemEntityMetaData
{
	public static final String ENTITY_NAME = "settings";
	public static final String ID = "id";

	public SettingsEntityMeta()
	{
		super(ENTITY_NAME);
		setAbstract(true);
		setPackage(SettingsPackage.INSTANCE);
		addAttribute(ID, ROLE_ID).setLabel("Id").setVisible(false);
	}
}
