package com.impetus.client.couchdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;

public class CouchDBQuery extends QueryImpl
{
    public CouchDBQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;
    }

    @Override
    protected List populateEntities(EntityMetadata m, Client client)
    {
        CouchDBQueryInterpreter interpreter = onTranslation(getKunderaQuery().getFilterClauseQueue(), m);
        return ((CouchDBClient) client).createAndExecuteQuery(interpreter);
    }

    @Override
    protected List recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();
        ls = populateEntities(m, client);
        return setRelationEntities(ls, client, m);
    }

    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    @Override
    protected EntityReader getReader()
    {
        return new CouchDBEntityReader();
    }

    @Override
    protected int onExecuteUpdate()
    {
        if (kunderaQuery.isDeleteUpdate())
        {
            List result = getResultList();
            return result != null ? result.size() : 0;
        }
        return 0;
    }

    @Override
    public void close()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

    @Override
    public Iterator iterate()
    {
        return new ResultIterator((CouchDBClient) persistenceDelegeator.getClient(getEntityMetadata()),
                getEntityMetadata(), persistenceDelegeator, onTranslation(getKunderaQuery().getFilterClauseQueue(),
                        getEntityMetadata()), getFetchSize() != null ? getFetchSize() : this.maxResult);
    }

    private CouchDBQueryInterpreter onTranslation(Queue clauseQueue, EntityMetadata m)
    {

        CouchDBQueryInterpreter interpreter = new CouchDBQueryInterpreter(getColumns(getKunderaQuery().getResult(), m),
                getMaxResults(), m);
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());

        EntityType entity = metaModel.entity(m.getEntityClazz());

        // If there is no clause present, means we might need to scan complete
        // table.
        /**
         * TODOOOO: Create a sorted set with table name. and add row key as
         * score and value on persist. delete it out as well on delete call.
         */
        for (Object clause : clauseQueue)
        {
            if (clause.getClass().isAssignableFrom(FilterClause.class))
            {
                Object value = ((FilterClause) clause).getValue();
                String condition = ((FilterClause) clause).getCondition();
                String columnName = ((FilterClause) clause).getProperty();
                Attribute col = entity.getAttribute(m.getFieldName(columnName));
                interpreter.setKeyValues(columnName,
                        PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(), value.getClass(), value));
                interpreter.setKeyName(columnName);
                if (columnName.equals(((AbstractAttribute) m.getIdAttribute()).getJPAColumnName()))
                {
                    interpreter.setIdQuery(true);
                }

                if (condition.equals("="))
                {
                    interpreter.setKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                            value.getClass(), value));
                }
                else if (condition.equals(">=") || condition.equals(">"))
                {
                    interpreter.setStartKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                            value.getClass(), value));
                }
                else if (condition.equals("<="))
                {
                    interpreter.setEndKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                            value.getClass(), value));
                }
                else if (condition.equals("<"))
                {
                    interpreter.setEndKeyValue(PropertyAccessorHelper.fromSourceToTargetClass(col.getJavaType(),
                            value.getClass(), value));
                    interpreter.setIncludeLastKey(false);
                }
                else
                {
                    throw new QueryHandlerException("Condition:" + condition + " not supported for CouchDB");
                }
            }
            else
            {
                String opr = clause.toString().trim();

                if (interpreter.getOperator() == null)
                {
                    if (opr.equalsIgnoreCase("AND"))
                    {
                        interpreter.setOperator("AND");
                    }
                    else if (opr.equalsIgnoreCase("OR"))
                    {
                        throw new QueryHandlerException("Invalid intra clause OR is not supported for CouchDB");
                    }
                    else
                    {
                        throw new QueryHandlerException("Invalid intra clause:" + opr + " not supported for CouchDB");
                    }
                }
                else if (interpreter.getOperator() != null && !interpreter.getOperator().equalsIgnoreCase(opr))
                {
                    throw new QueryHandlerException("Multiple combination of AND/OR clause not supported for CouchDB");
                }
                // it is a case of "AND", "OR" clause
            }
        }
        return interpreter;
    }
}