/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.mongodb;

import java.lang.reflect.Field;
import java.util.*;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.attributes.AttributeType;
import com.impetus.kundera.persistence.EntityReaderException;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Provides functionality for mapping between MongoDB documents and POJOs.
 * Contains utility methods for converting one form into another.
 * 
 * @author amresh.singh
 */
public class DocumentObjectMapper
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(DocumentObjectMapper.class);

    /**
     * Creates a MongoDB document object wrt a given Java object. columns in the
     * document correspond Columns provided as List.
     * 
     * @param obj
     *            the obj
     * @param columns
     *            the columns
     * @return the document from object
     * @throws PropertyAccessException
     *             the property access exception
     */
    static DBObject getDocumentFromObject(MetamodelImpl metaModel, Object obj, Set<Attribute> columns, Attribute idAttribute)
            throws PropertyAccessException
    {
        BasicDBObject dBObj = new BasicDBObject();

        for (Attribute column : columns)
        {
            if (!column.equals(idAttribute))
            {
                try
                {
                    AbstractAttribute attribute = (AbstractAttribute) column;
                    Field field = (Field) column.getJavaMember();
                    Object valueObject = PropertyAccessorHelper.getObject(obj, field);
                    Object value = null;
                    if (metaModel.isEmbeddable(attribute.getBindableJavaType()))
                    {
                        value = toEmbeddedValue(attribute, valueObject, metaModel);
                    }
                    else if (!attribute.isAssociation())
                    {
                        value = toFieldValue(column.getJavaType(), valueObject);
                    }
                    
                    if (value != null)
                    {
                        dBObj.put(attribute.getJPAColumnName(), value);
                    }
                }
                catch (PropertyAccessException paex)
                {
                    log.error("Can't access property " + column.getName());
                }
            }
        }
        return dBObj;
    }


    static Object toFieldValue(Class javaType, Object valueObject) throws PropertyAccessException
    {
        if (valueObject == null)
        {
            return null;
        }
        
        switch (AttributeType.getType(javaType))
        {
            case MAP:
                Map<?, ?> mapObj = (Map) valueObject;
                
                BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
                for (Map.Entry<?, ?> entry : mapObj.entrySet()) {
                    Object key = entry.getKey();
                    Object value = entry.getValue();
                    builder.add(MongoDBUtils.populateValue(key, key.getClass()).toString(), toFieldValue(value != null ? value.getClass() : null, value));
                }

                return builder.get();
            case SET:
            case LIST:
                Collection collection = (Collection) valueObject;
                
                BasicDBList basicDBList = new BasicDBList();
                for (Object o : collection)
                {
                    basicDBList.add(toFieldValue(o != null ? o.getClass() : null, o));
                }
                return basicDBList;
            case POINT:
                Point p = (Point) valueObject;
                return new double[] { p.getX(), p.getY() };
            case ENUM:
            case PRIMITIVE:
                return MongoDBUtils.populateValue(valueObject, javaType);
            default:
                return null; //TODO throw Exception
        }
    }

    static Object toEmbeddedValue(AbstractAttribute column, Object embeddedObject, MetamodelImpl metaModel)
    {
        EmbeddableType embeddableType = metaModel.embeddable(column.getBindableJavaType());
        Object value = null;

        if(embeddedObject != null)
        {
            if (column.isCollection())
            {
                Collection embeddedCollection = (Collection) embeddedObject;
                // means it is case of element collection

                DBObject[] dBObjects = new BasicDBObject[embeddedCollection.size()];
                int count = 0;
                for (Object o : embeddedCollection)
                {
                    dBObjects[count] = getDocumentFromObject(metaModel, o, embeddableType.getAttributes(), null);
                    count++;
                }
                value = dBObjects;
            }
            else
            {
                value = getDocumentFromObject(metaModel, embeddedObject, embeddableType.getAttributes(), null);
            }
        }

        return value;

    }

    /**
     * Creates an instance of <code>entityClass</code> and populates fields fetched
     * from MongoDB document object. Field names are determined from
     * <code>columns</code>
     * 
     * @param documentObj
     *            the document obj
     * @param entityClass
     *            the entityClass
     * @param columns
     *            the columns
     * @return the object from document
     */
    static Object getObjectFromDocument(MetamodelImpl metamodel, DBObject documentObj, Class entityClass,
            Set<Attribute> columns, SingularAttribute idAttribute)
    {
        try
        {
            Object entity = entityClass.newInstance();
            
            for (Attribute column : columns)
            {
                if (!column.equals(idAttribute))
                {
                    final AbstractAttribute attribute = (AbstractAttribute) column;
                    final Field field = (Field) column.getJavaMember();
                    final Class bindableJavaType = attribute.getBindableJavaType();
                    final Object fieldValue = documentObj.get(attribute.getJPAColumnName());
                    Object value = null;
                    
                    if (metamodel.isEmbeddable(bindableJavaType))
                    {
                        EmbeddableType embeddable = metamodel.embeddable(bindableJavaType);
                        if (column.isCollection())
                        {
                            Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(field);

                            value = fromEmbeddedCollection(metamodel, (BasicDBList) fieldValue,
                                    field.getType(), embeddedObjectClass, embeddable.getAttributes());
                        }
                        else
                        {
                            value = getObjectFromDocument(metamodel, (BasicDBObject) fieldValue,
                                    bindableJavaType, embeddable.getAttributes(), null);
                        }
                    }
                    else if (!column.isAssociation())
                    {
                        value = fromFieldValue(fieldValue, column.getJavaType());
                    }

                    try
                    {
                        if(value != null)
                        {
                            PropertyAccessorHelper.set(entity, field, value);
                        }
                    }
                    catch (PropertyAccessException paex)
                    {
                        log.error("Error while setting column {} value, caused by : .",
                                ((AbstractAttribute) column).getJPAColumnName(), paex);
                        throw new PersistenceException(paex);
                    }
                }
            }
            return entity;
        }
        catch (InstantiationException e)
        {
            throw new PersistenceException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new PersistenceException(e);
        }
    }

    /**
     *
     * @param value
     *            column value.
     * @param javaType
     *            field type.
     */
    static Object fromFieldValue(final Object value, Class javaType)
    {
        if (value == null)
        {
            return null;
        }

        AttributeType type = AttributeType.getType(javaType);
        switch (type)
        {
        case MAP:
            Map<?, ?> mapValues = ((BasicDBObject) value).toMap();
            
            Map mapResult = new HashMap();
            
            for(Map.Entry<?, ?> entry : mapValues.entrySet())
            {
                Object mapValue = entry.getValue();
                mapResult.put(entry.getKey(), mapValue == null ? null : fromFieldValue(mapValue, mapValue.getClass()));
            }
            return mapResult;
            
        case SET:
        case LIST:
            Object[] collectionValues = ((BasicDBList) value).toArray();
            Collection<Object> setResult = type == AttributeType.SET ? new HashSet<Object>() : new ArrayList<Object>();
            
            for(Object collectionValue : collectionValues)
            {
                setResult.add(collectionValue == null ? null : fromFieldValue(collectionValue, collectionValue.getClass()));
            }
            return setResult;
        case POINT:
            BasicDBList list = (BasicDBList) value;

            Object xObj = list.get(0);
            Object yObj = list.get(1);

            if (xObj != null && yObj != null)
            {
                try
                {
                    double x = Double.parseDouble(xObj.toString());
                    double y = Double.parseDouble(yObj.toString());

                    return new Point(x, y);
                }
                catch (NumberFormatException e)
                {
                    log.error("Error while reading geolocation data; Reason - possible corrupt data, Caused by : .", e);
                    throw new EntityReaderException("Error while reading geolocation data"
                            + "; Reason - possible corrupt data.", e);
                }
            }
            return null; //TODO
        case ENUM:
            EnumAccessor accessor = new EnumAccessor();
            return accessor.fromString(javaType, value.toString());
        case PRIMITIVE:
            Object populatedValue = MongoDBUtils.populateValue(value, value.getClass());
            return MongoDBUtils.getTranslatedObject(populatedValue, value.getClass(), javaType);
        default:
            return null; //TODO throw Exception
        }
    }

    /**
     * Creates a collection of <code>embeddedObjectClass</code> instances
     * wherein each element is java object representation of MongoDB document
     * object contained in <code>documentList</code>. Field names are determined
     * from <code>columns</code>.
     *
     * @param documentList
     *            the document list
     * @param embeddedCollectionClass
     *            the embedded collection class
     * @param embeddedObjectClass
     *            the embedded object class
     * @param columns
     *            the columns
     * @param metamodel
     * @return the collection from document list
     */
    static Collection<?> fromEmbeddedCollection(Metamodel metamodel, BasicDBList documentList,
                                                Class embeddedCollectionClass, Class embeddedObjectClass, Set<Attribute> columns)
    {
        Collection<Object> embeddedCollection = null;
        if (embeddedCollectionClass.equals(Set.class))
        {
            embeddedCollection = new HashSet<Object>();
        }
        else if (embeddedCollectionClass.equals(List.class))
        {
            embeddedCollection = new ArrayList<Object>();
        }
        else
        {
            throw new PersistenceException("Invalid collection class " + embeddedCollectionClass
                    + "; only Set and List allowed");
        }

        for (Object dbObj : documentList)
        {
            Object object = getObjectFromDocument((MetamodelImpl) metamodel, (BasicDBObject) dbObj, embeddedObjectClass, columns, null);
            embeddedCollection.add(object);
        }

        return embeddedCollection;
    }

}