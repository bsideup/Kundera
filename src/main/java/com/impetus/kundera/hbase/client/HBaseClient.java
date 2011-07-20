/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.hbase.client;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jasper.tagplugins.jstl.core.Set;

import com.impetus.kundera.Constants;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.hbase.admin.DataHandler;
import com.impetus.kundera.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.hbase.client.service.HBaseWriter;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EmbeddedCollectionCacheHandler;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient implements com.impetus.kundera.Client
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseClient.class);
    
    /** The contact node. */
    String contactNode;

    /** The default port. */
    String defaultPort;

    /** The handler. */
    private DataHandler handler;

    /** The is connected. */
    private boolean isConnected;

    /** The em. */
    private EntityManager em;

    @Override
    @Deprecated
    public void writeColumns(String keyspace, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)
            throws Exception
    {
        throw new NotImplementedException("Not yet implemented, Deprecated");
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#writeColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, com.impetus.kundera.proxy.EnhancedEntity,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public void writeColumns(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) throws Exception
    {
        String dbName = m.getSchema();          //Has no meaning for HBase, no used
        String tableName = m.getTableName();
        String rowKey = e.getId();
        //List<Column> columns = m.getColumnsAsList();   TODO: See how to handle this   
        
        
        //Check whether this table exists, if not create it
        List<String> columnFamilyNames = m.getSuperColumnFieldNames();
        handler.createTableIfDoesNotExist(tableName, columnFamilyNames.toArray(new String[0]));
        
        handler.writeData(tableName, m, e);
        
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, String rowKey,
            EntityMetadata m) throws Exception
    {
        //columnFamily has a different meaning for HBase, so it won't be used here
        String tableName = m.getTableName();
        E e = handler.readData(tableName, clazz, m, rowKey);
        return e;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily,
            EntityMetadata m, String... keys) throws Exception
    {
        List<E> entities = new ArrayList<E>();
        for (String rowKey : keys)
        {
            E e = handler.readData(m.getTableName(), clazz, m, rowKey);
            entities.add(e);
        }
        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, com.impetus.kundera.metadata.EntityMetadata,
     * java.util.Queue)
     */
    public <E> List<E> loadColumns(EntityManagerImpl em, EntityMetadata m, Query query) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#shutdown()
     */
    @Override
    public void shutdown()
    {
        handler.shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public void connect()
    {
        if (!isConnected)
        {
            handler = new HBaseDataHandler(contactNode, defaultPort);
            isConnected = true;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setContactNodes(java.lang.String[])
     */
    @Override
    public void setContactNodes(String... contactNodes)
    {
        this.contactNode = contactNodes[0];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setDefaultPort(int)
     */
    @Override
    public void setDefaultPort(int defaultPort)
    {
        this.defaultPort = String.valueOf(defaultPort);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    @Override
    public void delete(String keyspace, String columnFamily, String rowId) throws Exception
    {
        throw new RuntimeException("TODO:not yet supported");

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setKeySpace(String keySpace)
    {
        // TODO not required, Keyspace not applicable to Hbase
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#getType()
     */
    @Override
    public DBType getType()
    {
        return DBType.HBASE;
    }
}
