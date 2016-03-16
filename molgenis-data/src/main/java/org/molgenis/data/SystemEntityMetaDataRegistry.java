package org.molgenis.data;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.molgenis.data.meta.AttributeMetaDataMetaData;
import org.molgenis.data.meta.DefaultPackage;
import org.molgenis.data.meta.EntityMetaDataMetaData;
import org.molgenis.util.ApplicationContextProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Registry of {@link EntityMetaData Entity meta data} and {@link AttributeMetaData Attribute meta data} that are
 * defined in Java code.
 */
@Component
public class SystemEntityMetaDataRegistry
{
	private final DataService dataService; // TODO remove dependency
	private Map<String, EntityMetaData> systemEntityMap;
	private Map<String, AttributeMetaData> systemAttributeIdentifiers;

	@Autowired
	public SystemEntityMetaDataRegistry(DataService dataService)
	{
		this.dataService = requireNonNull(dataService);
	}

	public EntityMetaData getSystemEntity(String entityName)
	{
		if (systemEntityMap == null)
		{
			initSystemEntityMap();
		}

		return systemEntityMap.get(entityName);
	}

	public boolean isSystemEntity(String entityName)
	{
		if (systemEntityMap == null)
		{
			initSystemEntityMap();
		}

		return systemEntityMap.containsKey(entityName);
	}

	public AttributeMetaData getSystemAttribute(String attrIdentifier)
	{
		if (systemAttributeIdentifiers == null)
		{
			initSystemAttributeIdentifiers();
		}
		return systemAttributeIdentifiers.get(attrIdentifier);
	}

	/**
	 * Is this attribute referred to by a system entity?
	 * 
	 * @param attrEntity
	 * @return
	 */
	public boolean isSystemAttribute(String attrIdentifier)
	{
		if (systemAttributeIdentifiers == null)
		{
			initSystemAttributeIdentifiers();
		}

		return systemAttributeIdentifiers.containsKey(attrIdentifier);
	}

	public Package getSystemPackage(String packageName)
	{
		return packageName.equals(DefaultPackage.PACKAGE_NAME) ? DefaultPackage.INSTANCE : null;
	}

	public boolean isSystemPackage(String packageName)
	{
		return packageName.equals(DefaultPackage.PACKAGE_NAME);
	}

	private void initSystemEntityMap()
	{
		// system entities that are defined in Java code
		Map<String, EntityMetaData> entityMetaBeanNameMap = ApplicationContextProvider.getApplicationContext()
				.getBeansOfType(EntityMetaData.class);

		systemEntityMap = entityMetaBeanNameMap.values().stream()
				.collect(toMap(EntityMetaData::getName, Function.identity()));
	}

	private void initSystemAttributeIdentifiers()
	{
		if (systemEntityMap == null)
		{
			initSystemEntityMap();
		}

		systemAttributeIdentifiers = new HashMap<>();

		systemEntityMap.values().forEach(systemEntityMeta -> {
			Map<String, String> attrNameIdentifierMap = new HashMap<>();
			Entity systemEntity = dataService.findOne(EntityMetaDataMetaData.ENTITY_NAME, systemEntityMeta.getName());
			Iterable<Entity> systemEntityAttrs = systemEntity.getEntities(EntityMetaDataMetaData.ATTRIBUTES);
			createAttrNameIdentifierMapRec(systemEntityAttrs, attrNameIdentifierMap);

			initSystemAttributeIdentifiersRec(systemEntityMeta, systemEntityMeta.getOwnAttributes(),
					attrNameIdentifierMap);
		});
	}

	private void initSystemAttributeIdentifiersRec(EntityMetaData entityMeta, Iterable<AttributeMetaData> attrs,
			Map<String, String> attrNameIdentifierMap)
	{
		attrs.forEach(attr -> {
			String attrIdentifier = attrNameIdentifierMap.get(attr.getName());
			systemAttributeIdentifiers.put(attrIdentifier, attr);
			initSystemAttributeIdentifiersRec(entityMeta, attr.getAttributeParts(), attrNameIdentifierMap);
		});
	}

	private void createAttrNameIdentifierMapRec(Iterable<Entity> attrs, Map<String, String> attrNameIdentifierMap)
	{
		attrs.forEach(attr -> {
			String attrIdentifier = attr.getString(AttributeMetaDataMetaData.IDENTIFIER);
			String attrName = attr.getString(AttributeMetaDataMetaData.NAME);
			attrNameIdentifierMap.put(attrName, attrIdentifier);
			createAttrNameIdentifierMapRec(attr.getEntities(AttributeMetaDataMetaData.PARTS), attrNameIdentifierMap);
		});
	}
}
