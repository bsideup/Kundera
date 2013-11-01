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
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Metamodel;

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
    static BasicDBObject getDocumentFromObject(Metamodel metaModel, Object obj, Set<Attribute> columns)
            throws PropertyAccessException
    {
        BasicDBObject dBObj = new BasicDBObject();

        for (Attribute column : columns)
        {
            if (((MetamodelImpl) metaModel).isEmbeddable(((AbstractAttribute) column).getBindableJavaType()))
            {
                MongoDBDataHandler handler = new MongoDBDataHandler();
                handler.onEmbeddable(column, obj, metaModel, dBObj);
            }
            else
            {
                extractFieldValue(obj, dBObj, column);
            }
        }
        return dBObj;
    }

    /**
     * Creates a MongoDB document list from a given java collection. columns in
     * the document correspond Columns provided as List.
     * 
     * @param coll
     *            the coll
     * @param columns
     *            the columns
     * @return the document list from collection
     * @throws PropertyAccessException
     *             the property access exception
     */
    static BasicDBObject[] getDocumentListFromCollection(Metamodel metaModel, Collection coll, Set<Attribute> columns)
            throws PropertyAccessException
    {
        BasicDBObject[] dBObjects = new BasicDBObject[coll.size()];
        int count = 0;
        for (Object o : coll)
        {
            dBObjects[count] = getDocumentFromObject(metaModel, o, columns);
            count++;
        }
        return dBObjects;
    }

    /**
     * Creates an instance of <code>clazz</code> and populates fields fetched
     * from MongoDB document object. Field names are determined from
     * <code>columns</code>
     * 
     * @param documentObj
     *            the document obj
     * @param clazz
     *            the clazz
     * @param columns
     *            the columns
     * @return the object from document
     */
    static Object getObjectFromDocument(Metamodel metamodel, BasicDBObject documentObj, Class clazz,
            Set<Attribute> columns)
    {
        try
        {
            Object obj = clazz.newInstance();
            for (Attribute column : columns)
            {
                if (((MetamodelImpl) metamodel).isEmbeddable(((AbstractAttribute) column).getBindableJavaType()))
                {
                    MongoDBDataHandler handler = new MongoDBDataHandler();
                    handler.onViaEmbeddable(column, obj, metamodel, documentObj);
                }
                else
                {
                    setFieldValue(documentObj, obj, column);
                }
            }
            return obj;
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
     * Setter for column value, by default converted from string value, in case
     * of map it is automatically converted into map using BasicDBObject.
     *
     * @param document
     *            mongo document
     * @param entityObject
     *            searched entity.
     * @param column
     *            column field.
     */
    static void setFieldValue(DBObject document, Object entityObject, Attribute column)
    {
        Object fieldValue = null;
        if (document != null)
        {
            fieldValue = document.get(((AbstractAttribute) column).getJPAColumnName());
        }

        try
        {
            Class javaType = column.getJavaType();
            Object value = fromFieldValue(fieldValue, javaType);
            if(value != null)
            {
                PropertyAccessorHelper.set(entityObject, (Field) column.getJavaMember(), value);
            }
        }
        catch (PropertyAccessException paex)
        {
            log.error("Error while setting column {} value, caused by : .",
                    ((AbstractAttribute) column).getJPAColumnName(), paex);
            throw new PersistenceException(paex);
        }
    }

    static Object convertToFieldValue(Object valueObject, Class javaType) throws PropertyAccessException
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
                builder.add(MongoDBUtils.populateValue(key, key.getClass()).toString(), value == null ? null : convertToFieldValue(value, value.getClass()));
            }

            return builder.get();
        case SET:
        case LIST:
            Collection collection = (Collection) valueObject;
            BasicDBList basicDBList = new BasicDBList();
            for (Object o : collection)
            {
                basicDBList.add(o == null ? null : convertToFieldValue(o, o.getClass()));
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

    /**
     * Extract entity field.
     *
     * @param entity
     *            the entity
     * @param dbObj
     *            the db obj
     * @param column
     *            the column
     * @throws PropertyAccessException
     *             the property access exception
     */
    static void extractFieldValue(Object entity, DBObject dbObj, Attribute column) throws PropertyAccessException
    {
        Object valueObject = PropertyAccessorHelper.getObject(entity, (Field) column.getJavaMember());
        Class javaType = column.getJavaType();
        
        try
        {
            Object fieldValue = convertToFieldValue(valueObject, javaType);
            if(fieldValue != null)
            {
                dbObj.put(((AbstractAttribute) column).getJPAColumnName(), fieldValue);
            }
        }
        catch (PropertyAccessException paex)
        {
            log.error("Error while getting column {} value, caused by : .",
                    ((AbstractAttribute) column).getJPAColumnName(), paex);
            throw new PersistenceException(paex);
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
    static Collection<?> getCollectionFromDocumentList(Metamodel metamodel, BasicDBList documentList,
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
            embeddedCollection
                    .add(getObjectFromDocument(metamodel, (BasicDBObject) dbObj, embeddedObjectClass, columns));
        }

        return embeddedCollection;
    }
}