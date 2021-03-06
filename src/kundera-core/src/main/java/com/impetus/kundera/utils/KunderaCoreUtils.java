package com.impetus.kundera.utils;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;

import org.apache.commons.configuration.EnvironmentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.ProxyHelper;

public class KunderaCoreUtils
{

    private static final String COMPOSITE_KEY_SEPERATOR = "\001";

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaCoreUtils.class);

    /**
     * Retrun map of external properties for given pu;
     * 
     * @param pu
     * @param externalProperties
     * @param persistenceUnits
     * @return
     */
    public static Map<String, Object> getExternalProperties(String pu, Map<String, Object> externalProperties,
            String... persistenceUnits)
    {
        Map<String, Object> puProperty;
        if (persistenceUnits != null && persistenceUnits.length > 1 && externalProperties != null)
        {
            puProperty = (Map<String, Object>) externalProperties.get(pu);

            // if property found then return it, if it is null by pass it, else
            // throw invalidConfiguration.
            if (puProperty != null)
            {
                return fetchPropertyMap(puProperty);
            }
            return null;
        }
        return externalProperties;
    }

    /**
     * @param puProperty
     * @return
     */
    private static Map<String, Object> fetchPropertyMap(Map<String, Object> puProperty)
    {
        if (puProperty.getClass().isAssignableFrom(Map.class) || puProperty.getClass().isAssignableFrom(HashMap.class))
        {
            return puProperty;
        }
        else
        {
            throw new InvalidConfigurationException(
                    "For cross data store persistence, please specify as: Map {pu,Map of properties}");
        }
    }

    public static boolean isEmptyOrNull(Object o)
    {
        if (o == null)
        {
            return true;
        }
        
        if (!ProxyHelper.isProxyOrCollection(o))
        {
            if (PropertyAccessorHelper.isCollection(o.getClass()))
            {
                Collection c = (Collection) o;
                if (c.isEmpty())
                {
                    return true;
                }
            }
            else if (Map.class.isAssignableFrom(o.getClass()))
            {
                Map m = (Map) o;
                if (m.isEmpty())
                {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Prepares composite key as a redis key.
     * 
     * @param m
     *            entity metadata
     * @param metaModel
     *            meta model.
     * @param compositeKey
     *            composite key instance
     * @return redis key
     */
    public static String prepareCompositeKey(final EntityMetadata m, final MetamodelImpl metaModel,
            final Object compositeKey)
    {
        // EmbeddableType keyObject =
        // metaModel.embeddable(m.getIdAttribute().getBindableJavaType());

        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();

        // Arrays.sort(fields);

        StringBuilder stringBuilder = new StringBuilder();
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                // Attribute compositeColumn =
                // keyObject.getAttribute(f.getName());
                try
                {
                    String fieldValue = PropertyAccessorHelper.getString(compositeKey, f); // field
                                                                                           // value

                    // what if field value is null????
                    stringBuilder.append(fieldValue);
                    stringBuilder.append(COMPOSITE_KEY_SEPERATOR);
                }
                catch (IllegalArgumentException e)
                {
                    logger.error("Error during persist, Caused by:", e);
                    throw new PersistenceException(e);
                }
            }
        }

        if (stringBuilder.length() > 0)
        {
            stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMPOSITE_KEY_SEPERATOR));
        }
        return stringBuilder.toString();
    }

    public static String resolvePath(String input)
    {
        if (null == input)
        {
            return input;
        }

        // matching for 2 groups match ${VAR_NAME} or $VAR_NAME

        Pattern pattern = Pattern.compile("\\$\\{(\\w+)\\}|\\$(\\w+)");

        Matcher matcher = pattern.matcher(input); // get a matcher object
        StringBuffer sb = new StringBuffer();
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        while (matcher.find())
        {
            String envVarName = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            String envVarValue = config.getString(envVarName);
            matcher.appendReplacement(sb, envVarValue != null ? envVarValue : "");
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
