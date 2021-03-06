
#include "rm.h"

#include <algorithm>
#include <cstring>
#include <iostream>
RelationManager* RelationManager::_rm = 0;
RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
	rc = rbfm->createFile(getFileName(INDEX_TABLE_NAME));
	if(rc)
		return rc;
    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
	rc = insertTable(INDEX_TABLE_ID, 1, INDEX_TABLE_NAME);
    if (rc)
        return rc;


    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
	rc = insertColumns(INDEX_TABLE_ID, indexDescriptor);
    if (rc)
        return rc;

    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
	rc = rbfm->destroyFile(getFileName(INDEX_TABLE_NAME));
	if(rc)
		return rc;
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();
	
	//Delete from Index catalog?
   rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if(rc)
        return rc;
	rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
        if (rc)
            return rc;
    }
    
    for(int i = 0; i < attr.size(); i++)
        destroyIndex(tableName, attr[i].name);
		
	
    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	IndexManager *ix = IndexManager::instance();
    // Create the rbfm file to store the table
    string file = getIndexName(tableName, attributeName);
	rc = ix->createFile(file);
    if (rc)
	{
        //cout <<"CREATE FAILED" << endl;
		return rc;
	}
        
	FileHandle fileHandle;
	rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
	if(rc)
		return rc;
	
    // Insert the table into the Tables table (0 means this is not a system table)
    void *indexData = malloc(INDEX_RECORD_DATA_SIZE);
    RID rid;
	// Grab the table ID
	//This checks if tableName is in the catalog
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;
	prepareIndexRecordData(id, tableName, attributeName, indexData);
	rc = rbfm->insertRecord(fileHandle, indexDescriptor, indexData, rid);
	if (rc)
		return rc;
	free(indexData);
    rc = rbfm->closeFile(fileHandle);
    //insert attributes from the table 
   rc = rbfm->openFile(getFileName(tableName), fileHandle);
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    vector<string> projection;
    projection.push_back(attributeName);
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, projection, rbfm_si);
    IXFileHandle ixfileHandle;
    ix->openFile(file, ixfileHandle);
    //Loop through record descriptor to find correct Attribute
    int attrNum = 0;
    for(int i = 0; i < recordDescriptor.size(); i++)
    {
        if(strcmp(recordDescriptor[i].name.c_str(), attributeName.c_str()) == 0)
        {            
            attrNum = i;
            i = recordDescriptor.size();
        }
    }
    void *data = malloc(INDEX_COL_FILE_NAME_SIZE + sizeof(int));
    while((rbfm_si.getNextRecord(rid, data)) == SUCCESS)
	{
        /*switch(recordDescriptor[attrNum].type)
        {
            case TypeInt:
            ix->insertEntry
            case TypeReal:

            case TypeVarChar:
            
        }*/
        ix->insertEntry(ixfileHandle, recordDescriptor[attrNum], data + 1, rid);
	}
    free(data);
    ix->closeFile(ixfileHandle);
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    if(rc)
        return rc;
    return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	IndexManager *ix = IndexManager::instance();
	string file = getIndexName(tableName, attributeName);
	rc = ix->destroyFile(file);
	if(rc)
	{
		return rc;
	}
	FileHandle fileHandle;
	rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
	if(rc)
	{
		return rc;
	}
	int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;
	void* value = &id;
	// Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; 
	projection.push_back(INDEX_COL_ATTRIBUTE_NAME);
	rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
	
	RID rid;
	void* data = malloc (4 + INDEX_COL_FILE_NAME_SIZE);
	bool found = false;
	while((rbfm_si.getNextRecord(rid, data)) == SUCCESS || found == true)
	{
		int len;
		memcpy(&len,(char*)data + 1, sizeof(int));
		char* str = (char*)malloc(len);
		memcpy(str, (char*) data + sizeof(int) + 1, len);
		if(strcmp(str, attributeName.c_str()) == 0)
		{
            cout <<"found haha " << endl;
			found = true;
            
		}
        free(str);
	}
    if (rc)
        return rc;

    // Delete RID from table and close file
    rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
	if(rc)
		return rc;
    rc = rbfm->closeFile(fileHandle);
	if(rc)
		return rc;
    rc = rbfm_si.close();
	if(rc)
		return rc;
	return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) 
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
	//begin looking to insert into index

	
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
	
	if(rc)
		return rc;
	
	int32_t id;
	rc = getTableID(tableName, id);
	if(rc)
		return rc;
	
	
	 // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
	projection.push_back(INDEX_COL_ATTRIBUTE_NAME);
    void *value = &id;

    rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;
	IndexManager *ix = IndexManager::instance();
	IXFileHandle ixfileHandle;
    RID tempRid;
	void* newData = malloc(INDEX_COL_FILE_NAME_SIZE + 4);
    void* val = malloc(INDEX_COL_FILE_NAME_SIZE + 4);
    while(rbfm_si.getNextRecord(tempRid, newData) == SUCCESS)
	{
		//cout << id << endl;
		int len;
		memcpy(&len, (char*)newData + 1, sizeof(int));
		char* str = (char*)malloc(len + 1);
       
		memcpy(str, (char*) newData + sizeof(int) + 1, len);
        str[len] = '\0';
        //cout <<"STR:"  << str << endl; 
        //cout << "len" << len << endl;
		for(int i = 0; i < recordDescriptor.size(); i++)
		{
            //cout << "i = " << i << endl;
			if(strcmp(str, recordDescriptor[i].name.c_str()) == 0)
			{
				//cout <<"insert please" << endl;
                readAttribute(tableName, rid, recordDescriptor[i].name.c_str(), val);
                string file = getIndexName(tableName, recordDescriptor[i].name);
				ix->openFile(file, ixfileHandle);
				ix->insertEntry(ixfileHandle, recordDescriptor[i], (char*)val + 1, rid);
                ix->closeFile(ixfileHandle);
			}	
		}
        free(str);
	}
    free(newData);
    free(val);
    rbfm_si.close();
	rc = rbfm->closeFile(fileHandle);
	if(rc)
		return rc;
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    
    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;
    
    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;
    
    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;
    
    // Get tableID from tableName
    int32_t id;
    rc = getTableID(tableName, id);
    if(rc)
	return rc;

   // Open INDEX_TABLE_NAME
   FileHandle fileHandle2;
   rc = rbfm->openFile(INDEX_TABLE_NAME, fileHandle2);
   if(rc)
	return rc;

    // Scan INDEX_TABLE_NAME for associated index-file names
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(INDEX_COL_FILE_NAME);
    void *value = &id;
    RID scanRID;

    rc = rbfm->scan(fileHandle2, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if(rc)
	return rc;

    // For each index file associated with given table:
    //     deleteEntry of attribute corresponding to record
    IndexManager *ix = IndexManager::instance();
    IXFileHandle ixfileHandle;
    void *newData = malloc(INT_SIZE + INDEX_COL_FILE_NAME_SIZE);
    void *key = malloc(INT_SIZE + INDEX_COL_FILE_NAME_SIZE);

    while(rbfm_si.getNextRecord(scanRID, newData) != RBFM_EOF)
    {
	// get current index file name
	int len;
	memcpy(&len, (char*)newData + sizeof(char), INT_SIZE);
	char *indexFileName = (char*)malloc(len+1);
	memcpy(&indexFileName, (char*)newData + sizeof(char) + INT_SIZE, len);
	indexFileName[len] = '\0';

	// get current attribute name
	string attribute = (string)indexFileName;
	int size = attribute.size();
	attribute = attribute.substr( tableName.size()+2, size-3);

		// match attribute name to recordDescriptor's Attribute
		for(unsigned i=0; i<recordDescriptor.size(); i++)
		{
			if( strcmp( recordDescriptor[i].name.c_str(), attribute.c_str()) == 0)
			{
				// get the attribute key to be deleted from index
				rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attribute, key);
				if(rc)
				{
					free(newData);
					free(key);
					return rc;
				}
				// delete entry from index
				ix->openFile((string)indexFileName, ixfileHandle);
				rc = ix->deleteEntry(ixfileHandle, recordDescriptor[i], key, rid);
				ix->closeFile(ixfileHandle);
				if(rc)
				{
					free(newData);
					free(key);
					return rc;
				}
			}
		}
    }
    rbfm->closeFile(fileHandle2);

    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	if(rc)
		return rc;
    rc = rbfm->closeFile(fileHandle);
	if(rc)
		return rc;
	return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // will initialize to file-name
    void *value = &id;
	FileHandle fileHandle;

    // Get list of attribute file names from Index table
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the index file entries whose table-id equal this table's ID
    	projection.push_back(INDEX_COL_ATTRIBUTE_NAME);
	rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

	void *attrFileName = malloc(sizeof(char) + INT_SIZE + INDEX_COL_FILE_NAME_SIZE);
	IXFileHandle ixfileHandle;
   
	IndexManager *ix = IndexManager::instance();

	RID attrRID;
    while((rc = rbfm_si.getNextRecord(attrRID, attrFileName)) == SUCCESS)
    {
	    // get attribute file name string
	    int len;
	    memcpy(&len, (char*)attrFileName + sizeof(char), INT_SIZE);
	    char *str = (char*) malloc(len );
	    memcpy(str, (char*)attrFileName + INT_SIZE + sizeof(char), len);
        str[len] = '\0';
        cout << len << endl << str << endl;
	
	    for( int i=0; i< recordDescriptor.size(); i++)
	    {
		    void *oldValue = malloc(sizeof(char) + INT_SIZE + INDEX_COL_FILE_NAME_SIZE);
		
		    if( strcmp( recordDescriptor[i].name.c_str(), str) == 0)
		    {
                
			    // get old attribute value
                string file = getIndexName(tableName, recordDescriptor[i].name);
			    readAttribute( tableName, rid, recordDescriptor[i].name, oldValue);
			     // get attribute index file handle
       			ix->openFile(file, ixfileHandle); 
			    // delete old attribute value from index
			    ix->deleteEntry(ixfileHandle, recordDescriptor[i], (char*)oldValue + sizeof(char), rid);
			    ix->closeFile(ixfileHandle);
		    }
		    free(oldValue);
	    }
    }
	rbfm->closeFile(fileHandle);

    // rm updateRecord method
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    // Get list of attribute file names from Index table
    rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the index file entries whose table-id equal this table's ID
    	projection.push_back(INDEX_COL_ATTRIBUTE_NAME);
	rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(attrRID, attrFileName)) == SUCCESS)
    {
	// get attribute file name string
	int len;
	memcpy(&len, (char*)attrFileName + sizeof(char), INT_SIZE);
	char *str = (char*) malloc(len + 1);
	memcpy(str, (char*)attrFileName + INT_SIZE + sizeof(char), len);
    str[len] = '\0';
    cout << str << endl;
	
	    for( int i=0; i< recordDescriptor.size(); i++)
	    {
		    void *newValue = malloc(sizeof(char) + INT_SIZE + INDEX_COL_FILE_NAME_SIZE);
		
		    if( strcmp( recordDescriptor[i].name.c_str(), str) == 0)
		    {
             
			    // get old attribute value
                string file = getIndexName(tableName, recordDescriptor[i].name);
			    readAttribute( tableName, rid, recordDescriptor[i].name, newValue);
			     // get attribute index file handle
       			ix->openFile(file, ixfileHandle); 
			    // insert new attribute value into index
        		ix->insertEntry(ixfileHandle, recordDescriptor[i], newValue + 1, rid);
			    ix->closeFile(ixfileHandle);
		    }
		    free(newValue);
	    }
    }

    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();
		
    return rc;

}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
	vector<Attribute> id;
	Attribute attr;
	attr.name = INDEX_COL_TABLE_ID;
	attr.type = TypeInt;
	attr.length = (AttrLength)INT_SIZE;
	id.push_back(attr);
	
	attr.name = INDEX_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEX_COL_FILE_NAME_SIZE;
	id.push_back(attr);
	
	attr.name = INDEX_COL_ATTRIBUTE_NAME;
	attr.type = TypeVarChar;
	attr.length = (AttrLength)INDEX_COL_FILE_NAME_SIZE;
	id.push_back(attr);
	
	return id;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

void RelationManager::prepareIndexRecordData(int32_t id, const string &tableName, const string &attributeName, void* data)
{
	unsigned offset = 0;

    //cout << id << endl << tableName << endl << attributeName << endl;


	string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

	char null = 0;
	memcpy((char*) data + offset, &null, 1);
    offset += 1;
	
	memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
	
	memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
	memcpy((char*) data + offset, tableName.c_str(), file_name_len);
    offset += file_name_len;
	
	int32_t name_len = attributeName.length();
	
	memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
	memcpy((char*) data + offset, attributeName.c_str(), name_len);
    offset += name_len;
}
// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;
    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;   
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);
    
    real = tmp;
}
string RelationManager::getIndexName(const string &tableName, const string &attributeName)
{
    return tableName + "-" + attributeName + ".ix";
}
// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

//RM_IndexScanIterator //////////////////
RC RelationManager::indexScan(const string &tableName, const string &attributeName,
const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator)
{
    vector<Attribute> attr;
    getAttributes(tableName, attr);
    Attribute condAttr;
    for(int i = 0; i < attr.size(); i++)
    {
        if(strcmp(attr[i].name.c_str(), attributeName.c_str()) == 0)
        {
            condAttr.name = attributeName;
            condAttr.length = attr[i].length;
            condAttr.type = attr[i].type;
        }
    }
    IndexManager *ix = IndexManager::instance();
    string file = getIndexName(tableName, attributeName);
    ix->openFile(file, rm_IndexScanIterator.ixfileHandle);
    return ix->scan(rm_IndexScanIterator.ixfileHandle, condAttr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_iter);
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
    return ix_iter.getNextEntry(rid, key);
}
// Get next matching entry
RC RM_IndexScanIterator::close()
{
	RC rc;
    IndexManager *ix = IndexManager::instance();
    ix_iter.close();
    rc = ix->closeFile(ixfileHandle);
	return rc;
}
