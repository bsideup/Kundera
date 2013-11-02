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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.utils.MongoDBUtils;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Provides utility methods for handling data held in MongoDB.
 * 
 * @author amresh.singh
 */
public final class MongoDBDataHandler
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(MongoDBDataHandler.class);

    /**
     * Gets the entity from document.
     * 
     * @param entityClass
     *            the entity class
     * @param m
     *            the m
     * @param document
     *            the document
     * @param relations
     *            the relations
     * @return the entity from document
     */
    public Object getEntityFromDocument(Class<?> entityClass, EntityMetadata m, DBObject document,
            List<String> relations)
    {
        // Entity object
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        try
        {
            MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            Map<String, Object> relationValue = null;

            // Populate entity columns
            // List<Column> columns = m.getColumnsAsList();
            EntityType entityType = metamodel.entity(entityClass);

            Set<Attribute> columns = entityType.getAttributes();

            SingularAttribute idAttribute = m.getIdAttribute();

            entity = DocumentObjectMapper.getObjectFromDocument(metamodel, document, entityClass, columns, idAttribute);

            for (Attribute column : columns)
            {
                if (!column.equals(idAttribute))
                {
                    String fieldName = ((AbstractAttribute) column).getJPAColumnName();

                    Class javaType = ((AbstractAttribute) column).getBindableJavaType();
                    if (metamodel.isEmbeddable(javaType))
                    {
                    }
                    else if (!column.isAssociation())
                    {
                    }
                    else if (relations != null)
                    {
                        if (relationValue == null)
                        {
                            relationValue = new HashMap<String, Object>();
                        }

                        if (relations.contains(fieldName)
                                && !fieldName.equals(((AbstractAttribute) idAttribute).getJPAColumnName()))
                        {
                            Object colValue = document.get(fieldName);
                            if (colValue != null)
                            {
                                String colFieldName = m.getFieldName(fieldName);
                                Attribute attribute = colFieldName != null ? entityType.getAttribute(colFieldName)
                                        : null;
                                EntityMetadata relationMetadata = KunderaMetadataManager.getEntityMetadata(attribute
                                        .getJavaType());
                                colValue = MongoDBUtils.getTranslatedObject(colValue, colValue.getClass(),
                                        relationMetadata.getIdAttribute().getJavaType());
                            }
                            relationValue.put(fieldName, colValue);
                        }
                    }
                }
            }

            // Populate primary key column
            Object rowKey = document.get("_id");
            Class<?> rowKeyValueClass = rowKey.getClass();
            Class<?> idClass = null;
            idClass = idAttribute.getJavaType();
            rowKey = MongoDBUtils.populateValue(rowKey, idClass);

            if (metamodel.isEmbeddable(idAttribute.getBindableJavaType()))
            {
                EmbeddableType embeddable = metamodel.embeddable(idAttribute.getBindableJavaType());
                Iterator<Attribute> iter = embeddable.getAttributes().iterator();
                Object compoundKey = idAttribute.getBindableJavaType().newInstance();
                while (iter.hasNext())
                {
                    AbstractAttribute compositeAttrib = (AbstractAttribute) iter.next();
                    Object value = ((BasicDBObject) rowKey).get(compositeAttrib.getJPAColumnName());
                    PropertyAccessorHelper.set(compoundKey, (Field) compositeAttrib.getJavaMember(), value);
                }
                PropertyAccessorHelper.setId(entity, m, compoundKey);
            }
            else
            {
                rowKey = MongoDBUtils.getTranslatedObject(rowKey, rowKeyValueClass, idClass);
                PropertyAccessorHelper.setId(entity, m, rowKey);

            }

            if (relationValue != null && !relationValue.isEmpty())
            {
                EnhanceEntity e = new EnhanceEntity(entity, PropertyAccessorHelper.getId(entity, m), relationValue);
                return e;
            }
            else
            {
                return entity;
            }
        }
        catch (PersistenceException e)
        {
            log.error("Error while Getting entity from Document, Caused by: ", e);
            return entity;
        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + entityClass + ", Caused by: ", e);
            return entity;
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while Getting entity from Document, Caused by: ", e);
            return entity;
        }
        catch (PropertyAccessException e)
        {
            log.error("Error while Getting entity from Document, Caused by: ", e);
            return entity;
        }

    }

    /**
     * Gets the document from entity.
     * 
     *
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param relations
     *            the relations
     * @return the document from entity
     * @throws PropertyAccessException
     *             the property access exception
     */
    DBObject getDocumentFromEntity(EntityMetadata m, Object entity, List<RelationHolder> relations)
            throws PropertyAccessException
    {
        // List<Column> columns = m.getColumnsAsList();

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());

        // Populate Row Key
        Object id = PropertyAccessorHelper.getId(entity, m);

        SingularAttribute idAttribute = m.getIdAttribute();
        Set<Attribute> columns = entityType.getAttributes();

        DBObject dbObj = DocumentObjectMapper.getDocumentFromObject(metaModel, entity, columns, idAttribute);

        if (metaModel.isEmbeddable(idAttribute.getBindableJavaType()))
        {
            MongoDBUtils.populateCompoundKey(dbObj, m, metaModel, id);
        }
        else
        {
            dbObj.put("_id", MongoDBUtils.populateValue(id, id.getClass()));
        }

        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                dbObj.put(rh.getRelationName(),
                        MongoDBUtils.populateValue(rh.getRelationValue(), rh.getRelationValue().getClass()));
            }
        }

        if (((AbstractManagedType) entityType).isInherited())
        {
            String discrColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
            String discrValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

            // No need to check for empty or blank, as considering it as valid
            // name for nosql!
            if (discrColumn != null && discrValue != null)
            {
                dbObj.put(discrColumn, discrValue);
            }
        }

        return dbObj;
    }

    /**
     * Returns column name from the filter property which is in the form
     * dbName.columnName
     * 
     * @param filterProperty
     *            the filter property
     * @return the column name
     */
    private String getColumnName(String filterProperty)
    {
        StringTokenizer st = new StringTokenizer(filterProperty, ".");
        String columnName = "";
        while (st.hasMoreTokens())
        {
            columnName = st.nextToken();
        }

        return columnName;
    }

    /**
     * Retrieves A collection of embedded object within a document that match a
     * criteria specified in <code>query</code> TODO: This code requires a
     * serious overhawl. Currently it assumes that user query is in the form
     * "Select alias.columnName from EntityName alias". However, correct query
     * to be supported is
     * "Select alias.superColumnName.columnName from EntityName alias"
     * 
     * @param dbCollection
     *            the db collection
     * @param m
     *            the m
     * @param documentName
     *            the document name
     * @param mongoQuery
     *            the mongo query
     * @param result
     *            the result
     * @param orderBy
     *            the order by
     * @param maxResult
     * @return the embedded object list
     * @throws PropertyAccessException
     *             the property access exception
     */
    List getEmbeddedObjectList(DBCollection dbCollection, EntityMetadata m, String documentName,
            BasicDBObject mongoQuery, String result, BasicDBObject orderBy, int maxResult, BasicDBObject keys)
            throws PropertyAccessException
    {
        List list = new ArrayList();// List of embedded object to be returned

        // MongoDBQuery mongoDBQuery = (MongoDBQuery) query;

        // Specified after entity alias in query
        String columnName = result /* getColumnName(result) */;

        // Something user didn't specify and we have to derive
        // TODO: User must specify this in query and remove this logic once
        // query format is changed
        // String enclosingDocumentName = getEnclosingDocumentName(m,
        // columnName);

        String enclosingDocumentName = null;

        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entityType = metaModel.entity(m.getEntityClazz());
        EmbeddableType superColumn = null;
        Set<Attribute> columns = null;
        Attribute attrib = null;
        try
        {
            attrib = entityType.getAttribute(columnName);
            // if (!m.getColumnFieldNames().contains(columnName))
            // {
            Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(m.getEntityClazz());
            for (String key : embeddables.keySet())
            // for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
            {
                superColumn = embeddables.get(key);
                // List<Column> columns = superColumn.getColumns();
                columns = superColumn.getAttributes();

                for (Attribute column : columns)
                {
                    if (((AbstractAttribute) column).getJPAColumnName().equals(columnName))
                    {
                        enclosingDocumentName = key;
                        break;
                    }
                }
            }
        }
        catch (IllegalArgumentException iax)
        {
            log.info("No column found for: " + columnName);
        }

        // Query for fetching entities based on user specified criteria
        DBCursor cursor = orderBy != null ? dbCollection.find(mongoQuery, keys).sort(orderBy) : dbCollection.find(
                mongoQuery, keys).limit(maxResult);

        // EmbeddableType superColumn =
        // m.getEmbeddedColumn(enclosingDocumentName);

        if (superColumn != null)
        {
            Field superColumnField = (Field) attrib.getJavaMember();
            while (cursor.hasNext())
            {
                DBObject fetchedDocument = cursor.next();
                Object embeddedDocumentObject = fetchedDocument.get(superColumnField.getName());

                if (embeddedDocumentObject != null)
                {
                    if (embeddedDocumentObject instanceof BasicDBList)
                    {
                        Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(superColumnField);
                        for (Object dbObj : (BasicDBList) embeddedDocumentObject)
                        {
                            Object embeddedObject = DocumentObjectMapper.getObjectFromDocument(metaModel,
                                    (BasicDBObject) dbObj, embeddedObjectClass, superColumn.getAttributes(), null);
                            Object fieldValue = PropertyAccessorHelper.getObject(embeddedObject, columnName);
                        }
                    }
                    else if (embeddedDocumentObject instanceof BasicDBObject)
                    {
                        Object embeddedObject = DocumentObjectMapper.getObjectFromDocument(metaModel,
                                (BasicDBObject) embeddedDocumentObject, superColumn.getJavaType(),
                                superColumn.getAttributes(), null);
                        list.add(embeddedObject);

                    }
                    else
                    {
                        throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                                + "it wasn't stored as BasicDBObject, possible problem in format.");
                    }
                }
            }
        }
        return list;
    }

}