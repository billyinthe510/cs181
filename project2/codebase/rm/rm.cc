#include "rm.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/stat.h>

RelationManager* RelationManager::_rm = 0;

RM_ScanIterator::RM_ScanIterator()
{
	cursor = 0;
}
RM_ScanIterator::~RM_ScanIterator()
{
	close();
}
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	if(cursor < rids.size() )
	{
		rid = rids[cursor];
		rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
		cursor++;
	}
	else
		return RM_EOF;
	return SUCCESS;
}
RC RM_ScanIterator::close()
{
	cursor = 0;
	while(rids.size() > 0 )
		rids.pop_back();
	while(recordDescriptor.size() > 0)
		recordDescriptor.pop_back();
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->closeFile(fileHandle);
	return SUCCESS;
}

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
    
    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    
    Attribute attr;
    unsigned int tableLen = 0;
    vector<Attribute> table_Data;
    // Table
    
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = AttrLength(4);
    table_Data.push_back(attr);
    tableLen += attr.length;
    
    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = AttrLength(50);
    table_Data.push_back(attr);
    tableLen += attr.length;
    
    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = AttrLength(50);
    table_Data.push_back(attr);
    tableLen += attr.length;
    
    // System or User table
    attr.name = "table-type";
    attr.type = TypeVarChar;
    attr.length = AttrLength(6);
    table_Data.push_back(attr);
    tableLen += attr.length;
    
    // Column
    Attribute attr2;
    vector<Attribute> column_Data;
    unsigned int columnLen = 0;
    
    attr2.name = "table-id";
    attr2.type = TypeInt;
    attr2.length = sizeof(int);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-name";
    attr2.type = TypeVarChar;
    attr2.length = AttrLength(50);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-type";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-length";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-position";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RID rid;
    string fileName1 = ("Tables");
    string fileName2 = ("Columns");
    
    
    // Check if "Tables" exists already
    struct stat stFileInfo;
    if (stat(fileName1.c_str(), &stFileInfo) == 0)
        return -1;
    
    // Create "Tables"
    rbfm->createFile(fileName1);
    rbfm->openFile(fileName1, fileHandle);
    
    void *table = malloc(tableLen + (3 * sizeof(int)));
    unsigned int t_Offset = 0;
    unsigned int t_ID = 1;
    unsigned int t_nameLen = fileName1.length();
    string t_Type = "system";
    unsigned int t_typeLen = t_Type.length();
    
    
    // Null-indicators
    int nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
    // Null-indicator for the fields
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy((char *)table + t_Offset, nullsIndicator, nullFieldsIndicatorActualSize);
    t_Offset += nullFieldsIndicatorActualSize;
    
    // table-id
    memcpy((char *)table + t_Offset, &t_ID, sizeof(int));
    t_Offset += sizeof(int);
    cout << "TABLE ID: " <<t_ID << endl;
    // table-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_nameLen << endl;
    memcpy((char*)table + t_Offset, fileName1.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    cout << t_Offset << endl;
    // file-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)table + t_Offset, fileName1.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    cout << t_Offset << endl;
    // table-type
    memcpy((char *)table + t_Offset, &t_typeLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)table + t_Offset, t_Type.c_str(), t_typeLen);
    t_Offset +=  t_typeLen;
    cout << t_Offset << "LAST" << endl;
    
    rbfm->insertRecord(fileHandle, table_Data, table, rid);
    //rbfm->closeFile(fileHandle);
    //free(table);
    
    void *column = malloc(tableLen + (3 * sizeof(int)));;
    t_Offset = 0;
    t_ID = 2;
    t_nameLen = fileName2.length();
    //system = catalog user = user made table
    t_Type = "system";
    t_typeLen = t_Type.length();
    
    // Null-indicators
    nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
    // Null-indicator for the fields
    nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy((char *)column + t_Offset, nullsIndicator, nullFieldsIndicatorActualSize);
    t_Offset += nullFieldsIndicatorActualSize;
    
    // table-id
    cout << t_Offset << endl;
    memcpy((char *)column + t_Offset, &t_ID, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    // table-name
    memcpy((char *)column + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)column + t_Offset, fileName2.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    // file-name
    memcpy((char *)column + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)column + t_Offset, fileName2.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    cout << t_Offset << endl;
    // table-type
    memcpy((char *)column + t_Offset, &t_typeLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)column + t_Offset, t_Type.c_str(), t_typeLen);
    t_Offset +=  t_typeLen;
    cout << t_Offset << endl;
    rbfm->insertRecord(fileHandle, table_Data, column, rid);
    
    void *test = malloc(100);
    void *buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(0, buffer);
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, buffer , sizeof(SlotDirectoryHeader));
    //cout << slotHeader.recordEntriesNumber << endl;
    rid.slotNum = slotHeader.recordEntriesNumber - 1;
    rid.pageNum = 0;
    
    rbfm->readRecord(fileHandle, table_Data, rid, test);
    rbfm->printRecord(table_Data, test);
    rbfm->closeFile(fileHandle);
    //free(column);
    
    // Check if "Columns" exists already
    if (stat(fileName2.c_str(), &stFileInfo) == 0)
        return -1;
    
    // Create "Columns"
    //FileHandle fileHandle1;
    rbfm->createFile(fileName2);
    rbfm->openFile(fileName2, fileHandle);
    
    // Populate
    
    for (int i = 0; i < 8; i++)
    {
        //cout << columnLen;
        void *col_Data = malloc(columnLen + sizeof(int));
        unsigned int tab_ID;
        string col_Name;
        unsigned int col_Type;
        unsigned int col_Len;
        unsigned int col_Pos;
        unsigned col_Name_len;
        unsigned offset = 0;
        nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
        
        // Null-indicator for the fields
        //nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
        memcpy((char *)col_Data + offset, nullsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;
        
        switch(i)
        {
            case 0: // (1, "table-id", TypeInt, 4 , 1)
            {
                tab_ID = 1;
                col_Name = "table-id";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 1;
                col_Name_len = col_Name.length();
                break;
            }
            case 1: // (1, "table-name", TypeVarChar, 50, 2)
            {
                tab_ID = 1;
                col_Name = "table-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 2;
                col_Name_len = col_Name.length();
                break;
            }
            case 2: // (1, "file-name", TypeVarChar, 50, 3)
            {
                tab_ID = 1;
                col_Name = "file-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 3;
                col_Name_len = col_Name.length();
                break;
            }
            case 3: // (2, "table-id", TypeInt, 4, 1)
            {
                tab_ID = 2;
                col_Name = "table-id";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 1;
                col_Name_len = col_Name.length();
                break;
            }
            case 4: // (2, "column-name", TypeVarChar, 50, 2)
            {
                tab_ID = 2;
                col_Name = "column-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 2;
                col_Name_len = col_Name.length();
                break;
            }
            case 5: // (2, "column-type", TypeInt, 4, 3)
            {
                tab_ID = 2;
                col_Name = "column-type";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 3;
                col_Name_len = col_Name.length();
                break;
            }
            case 6: // (2, "column-length", TypeInt, 4, 4)
            {
                tab_ID = 2;
                col_Name = "column-length";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 4;
                col_Name_len = col_Name.length();
                break;
            }
            case 7: // (2, "column-position", TypeInt, 4, 5)
            {
                tab_ID = 2;
                col_Name = "column-position";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 5;
                col_Name_len = col_Name.length();
                break;
            }
        }
        memcpy((char *)col_Data + offset, &tab_ID, sizeof(int));
        offset += sizeof(int);
        
        memcpy((char*)col_Data + offset, &col_Name_len, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)col_Data + offset,  col_Name.c_str(), col_Name.length());
        offset +=  col_Name.length();
        
        memcpy((char*)col_Data + offset,  &col_Type, sizeof(int));
        offset +=  sizeof(int);
        
        memcpy((char*) col_Data + offset, &col_Len, sizeof(int));
        offset +=  sizeof(int);
        
        memcpy((char*) col_Data + offset, &col_Pos, sizeof(int));
        offset +=  sizeof(int);
        
        rbfm->insertRecord(fileHandle, column_Data, col_Data, rid);
        free(col_Data);
    }
    /*void *test = malloc(100);
     void *buffer = malloc(PAGE_SIZE);
    	SlotDirectoryHeader slotHeader;
     memcpy (&slotHeader, buffer, sizeof(SlotDirectoryHeader));
     rid.slotNum = slotHeader.recordEntriesNumber - 1;
     rid.pageNum = 0;
     
     rbfm->readRecord(fileHandle, column_Data, rid, test);
     rbfm->printRecord(column_Data, test);*/
    rbfm->closeFile(fileHandle);
    free(column);
    free(table);
    free(nullsIndicator);
    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    string fileName1 = ("Tables");
    string fileName2 = ("Columns");
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Check if "Tables" exists already
    struct stat stFileInfo;
    if (stat(fileName1.c_str(), &stFileInfo) != 0 )
        return -1;
    // Delete "Tables" table
    rbfm->destroyFile(fileName1);
    // Check if "Columns" exists already
    if (stat(fileName2.c_str(), &stFileInfo) != 0)
        return -1;
    // Delete "Columns" table
    rbfm->destroyFile(fileName2);
    
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    struct stat stFileInfo;
    if (stat(tableName.c_str(), &stFileInfo) == 0)
        return -1;
    FileHandle fileHandle;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    string fileName1 = ("Tables");
    string fileName2 = ("Columns");
    rbfm->createFile(tableName);
    Attribute attrCat;
    
    unsigned int tableLen = 0;
    
    // Catalog recordDescriptor. Using it to get the next tableId
    vector<Attribute> table_Data;
    
    attrCat.name = "table-id";
    attrCat.type = TypeInt;
    attrCat.length = AttrLength(4);
    table_Data.push_back(attrCat);
    tableLen += attrCat.length;
    
    attrCat.name = "table-name";
    attrCat.type = TypeVarChar;
    attrCat.length = AttrLength(50);
    table_Data.push_back(attrCat);
    tableLen += attrCat.length;
    
    attrCat.name = "file-name";
    attrCat.type = TypeVarChar;
    attrCat.length = AttrLength(50);
    table_Data.push_back(attrCat);
    tableLen += attrCat.length;
    // System or User table
    attrCat.name = "table-type";
    attrCat.type = TypeVarChar;
    attrCat.length = AttrLength(6);
    table_Data.push_back(attrCat);
    tableLen += attrCat.length;
    // Column
    vector<Attribute> column_Data;
    Attribute attr2;
    unsigned int columnLen = 0;
    
    attr2.name = "table-id";
    attr2.type = TypeInt;
    attr2.length = sizeof(int);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-name";
    attr2.type = TypeVarChar;
    attr2.length = AttrLength(50);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-type";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-length";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    attr2.name = "column-position";
    attr2.type = TypeInt;
    attr2.length = AttrLength(4);
    column_Data.push_back(attr2);
    columnLen += attr2.length;
    
    
    //first update Catalog file Tables
    cout << "WHY: " << tableLen << endl;
    rbfm->openFile(fileName1, fileHandle);
    void *buffer = malloc(PAGE_SIZE);
    int numPages = fileHandle.getNumberOfPages();
    fileHandle.readPage(numPages - 1, buffer);
    //get the slotHeader
    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, buffer, sizeof(SlotDirectoryHeader));
    int t_ID = slotHeader.recordEntriesNumber;
    t_ID++;
    
    void *table = malloc(tableLen + (3 * sizeof(int)));
    unsigned int t_Offset = 0;
    
    RID rid;
    unsigned int t_nameLen = tableName.length();
    string t_Type = "user";
    unsigned int t_typeLen = t_Type.length();
    // Null-indicators
    int nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
    // Null-indicator for the fields
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy((char *)table + t_Offset, nullsIndicator, nullFieldsIndicatorActualSize);
    t_Offset += nullFieldsIndicatorActualSize;
    
    // table-id
    memcpy((char *) table + t_Offset, &t_ID, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    // table-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)table + t_Offset, tableName.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    cout << t_Offset << endl;
    // file-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)table + t_Offset, tableName.c_str(), t_nameLen);
    t_Offset += t_nameLen;
    cout << t_Offset << endl;
    // table-type
    memcpy((char *)table + t_Offset, &t_typeLen, sizeof(int));
    t_Offset += sizeof(int);
    cout << t_Offset << endl;
    memcpy((char*)table + t_Offset, t_Type.c_str(), t_typeLen);
    t_Offset +=  t_typeLen;
    cout << "LAST " << t_Offset << endl;
    rbfm->insertRecord(fileHandle, table_Data, table, rid);
    
    //debug code
    void *test = malloc(100);
    //SlotDirectoryHeader slotHeader;
    //memcpy (&slotHeader, buffer, sizeof(SlotDirectoryHeader));
    
    rid.slotNum = slotHeader.recordEntriesNumber;
    rid.pageNum = 0;
    
    rbfm->readRecord(fileHandle, table_Data, rid, test);
    rbfm->printRecord(table_Data, test);
    
    fileHandle.readPage(0, buffer);
    memcpy(&slotHeader, buffer, sizeof(SlotDirectoryHeader));
    //free(buffer);
    free(table);
    rbfm->closeFile(fileHandle);
    unsigned int col_Pos = 0;
    rbfm->openFile(fileName2, fileHandle);
    //update slot header
    
    //add to Catalog file Columns
    
    for (unsigned i = 0; i < attrs.size(); i++)
    {
        //cout << columnLen;
        void *col_Data = malloc(columnLen + sizeof(int));
        unsigned int tab_ID;
        string col_Name;
        unsigned int col_Type;
        unsigned int col_Len;
        
        unsigned col_Name_len;
        unsigned offset = 0;
        nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
        
        // Null-indicator for the fields
        //nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
        memcpy((char *)col_Data + offset, nullsIndicator, nullFieldsIndicatorActualSize);
        offset += nullFieldsIndicatorActualSize;
        
        
        tab_ID = slotHeader.recordEntriesNumber;
        col_Name = attrs[i].name;
        col_Type = attrs[i].type;
        col_Len = attrs[i].length;
        col_Pos++;
        col_Name_len = col_Name.length();
        vector<Attribute> test2;
        test2.push_back(attrs[i]);
        memcpy((char *)col_Data + offset, &tab_ID, sizeof(int));
        offset += sizeof(int);
        
        memcpy((char*)col_Data + offset, &col_Name_len, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)col_Data + offset,  col_Name.c_str(), col_Name.length());
        offset +=  col_Name.length();
        
        memcpy((char*)col_Data + offset,  &col_Type, sizeof(int));
        offset +=  sizeof(int);
        
        memcpy((char*) col_Data + offset, &col_Len, sizeof(int));
        offset +=  sizeof(int);
        
        memcpy((char*) col_Data + offset, &col_Pos, sizeof(int));
        offset +=  sizeof(int);
        
        rbfm->insertRecord(fileHandle, column_Data, col_Data, rid);
        //debug code
        void* test1 = malloc(100);
        //ColumnOffset test3 = attrs[i].name.length();
        //memcpy((char *) test1, &test3, sizeof(ColumnOffset));
        rbfm->readRecord(fileHandle, column_Data, rid, test1);
        //rbfm->readAttribute(fileHandle, column_Data, rid, attrs[i].name, test1);
        //for(int j = 0; j < 10; j++)
        //cout << attrs[i].name << endl;
        //cout << rid1.slotNum << endl;
        if(rbfm->printRecord(column_Data, test1))
	{
		cout<<"couldn't print"<<endl;
		free(col_Data);
		free(nullsIndicator);
		return -1;
	}
        free(col_Data);
    }
    free(nullsIndicator);
    
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    string fileName1 = ("Tables");
    string fileName2 = ("Columns");
    string t_Name = tableName;
    int tableID;
    
    // Check if table exists
    struct stat stFileInfo;
    if (stat(tableName.c_str(), &stFileInfo) != 0)
        return -1;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RM_ScanIterator rmsi;
    
    RID rid;
    void *page = malloc(PAGE_SIZE);
    
    // Get table-id of tableName from Tables table
    if (rbfm->openFile(fileName1, fileHandle) != 0)
    {
        return -1;
    }
    vector<string> t_ID = {"table-id"};
    
    int t_NameLength = tableName.length();
    void *data = malloc(sizeof(int) + t_NameLength);
    memcpy((char *) data, &t_NameLength, sizeof(int));
    memcpy((char *) data + sizeof(int), tableName.c_str(), t_NameLength);
    
    if (scan(fileName1, "table-name", EQ_OP, data, t_ID, rmsi) != 0)
    {
        free(page);
        free(data);
        rbfm->closeFile(fileHandle);
        rmsi.close();
    }
    if (rmsi.getNextTuple(rid, page) != RM_EOF)
    {
        int offset = 0;
        // table-id
        memcpy(&tableID, (char*)page + offset, sizeof(int));
    }
    rmsi.close();
    t_ID.clear();
    
    // Get the recordDescriptor
    vector<Attribute> recordDescriptor;
    if (getAttributes(fileName1, recordDescriptor) != 0)
    {
        return -1;
    }
    if(rbfm->deleteRecord(fileHandle, recordDescriptor, rid))
   	{
		cout<<"failed to delete"<<endl;
		return -1;
	}
	 if(rbfm->closeFile(fileHandle))
	{
		cout<<"failed to close file"<<endl;
		return -1;
	}
    
    
    void *data2 = malloc(sizeof(int));
    memcpy((char *) data2, &tableID, sizeof(int));
    
    // Columns table
    if (rbfm->openFile(fileName2, fileHandle) != 0)
    {
        return -1;
    }
    
    // Get the recordDescriptor
    vector<Attribute> recordDescriptor2;
    if (getAttributes(fileName2, recordDescriptor2) != 0)
    {
        return -1;
    }
    
    if (scan(fileName2, "table-id", EQ_OP, data2, t_ID, rmsi) != 0)
    {
        free(data2);
        rbfm->closeFile(fileHandle);
        rmsi.close();
        return -1;
    }
    
    // Delete tuples of tableName in Columns table
    while (rmsi.getNextTuple(rid, data2) != RM_EOF)
    {
        rbfm->deleteRecord(fileHandle, recordDescriptor2, rid);
    }
    free(page);
    free(data);
    free(data2);
    rmsi.close();
    
    // Delete tableName file
   	 if(rbfm->destroyFile(tableName))
	{
	cout<<"failed to destroy table!"<<endl;
	return -1;
	}
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
 	// Make sure attrs vector is empty
	while(attrs.size() > 0 )
		attrs.pop_back();

    // rbfm->open("Tables")
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator rmsi;
    if(rbfm->openFile("Tables", rmsi.fileHandle))
        return RBFM_OPEN_FAILED;
    // WHERE table-name == tableName
    string conditionAttribute = "table-name";
    CompOp compOp = EQ_OP;
	uint32_t varlen = tableName.length();
    void *value = malloc(VARCHAR_LENGTH_SIZE + varlen);
    memcpy(value, &varlen, VARCHAR_LENGTH_SIZE ); 
    memcpy((char*)value + VARCHAR_LENGTH_SIZE, tableName.c_str(), varlen );
    // SELECT table-id
    vector<string> attrNames;
    attrNames.push_back("table-id");
    
    // Create Tables recordDescriptor
    Attribute attr;
    
	vector<Attribute> recordDescriptor;
    // table-id
    attr.name = "table-id";
    attr.length = AttrLength(4);
    attr.type = TypeInt;
    recordDescriptor.push_back(attr);
    
    // table-name
    attr.name = "table-name";
    attr.length = AttrLength(50);
    attr.type = TypeVarChar;
    recordDescriptor.push_back(attr);
    
    // file-name
    attr.name = "file-name";
    attr.length = AttrLength(50);
    attr.type = TypeVarChar;
    recordDescriptor.push_back(attr);
    
    // Scan for table-id for tableName in Tables
    if(rbfm->scan(rmsi.fileHandle, recordDescriptor,conditionAttribute,compOp,value,
               attrNames,rmsi))
	{
		cout<<"scan of Tables failed"<<endl;
		return -1;
	}
 
    free(value);
    RID rid;
    int tableID = -1;
    void *data = malloc(PAGE_SIZE);
    
    while(rmsi.getNextRecord(rid,data) != RBFM_EOF)
    {
        char *nullIndicator = (char*)malloc(sizeof(char));
	memcpy(nullIndicator, (char*)data, sizeof(char) );
        bool nullbit = nullIndicator[0] & ( 1 << 7);
        // If nullbit is null, table-id is NULL
        if(nullbit)
        {

cout<<"table-id is NULL!"<<endl;
            free(data);
            return -1;

        }
       
        // Otherwise get the table-id
        tableID = *( (int*) ((char*)data + sizeof(char)) );
        // Multiple table-ids associated with tableName
        if(rmsi.getNextRecord(rid,data) != RBFM_EOF)
        {
            free(data);
            return -1;
        }
    }
    rmsi.close();

    free(data);
    // tableName not found in Catalog
    if(tableID == -1)
        return -1;
    // Open Columns file
    if(rbfm->openFile("Columns",rmsi.fileHandle))
    {
        // Columns does not exist!
        return RBFM_OPEN_FAILED;
    }
    // WHERE table-id == tableID
    conditionAttribute = "table-id";
    compOp = EQ_OP;
    value = malloc(INT_SIZE);
    memcpy(value, &tableID, INT_SIZE );
    // SELECT column-name, column-type, column-length
    attrNames.pop_back();
    string attributes = "column-name";
    attrNames.push_back(attributes);
    attributes = "column-type";
    attrNames.push_back(attributes);
    attributes = "column-length";
    attrNames.push_back(attributes);
    
  	while(recordDescriptor.size() > 0 )
		recordDescriptor.pop_back();

    // table-id
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = AttrLength(4);
    recordDescriptor.push_back(attr);
    
    // column-name
    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = AttrLength(50);
    recordDescriptor.push_back(attr);
    
    // column-type
    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = AttrLength(4);
    recordDescriptor.push_back(attr);
    
    // column-length
    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = AttrLength(4);
    recordDescriptor.push_back(attr);
    
    // column-position
    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = AttrLength(4);
    recordDescriptor.push_back(attr);
    if(rbfm->scan(rmsi.fileHandle, recordDescriptor, conditionAttribute,compOp,value,
               attrNames,rmsi))
	return -1;
    free(value);
	data = malloc(PAGE_SIZE); 
   while(rmsi.getNextRecord(rid,data) != RBFM_EOF)
    {
        // get null byte
        char *nullField = (char*) malloc( sizeof(char) );
        memset(nullField, 0, sizeof(char) );
        memcpy(nullField, (char*)data, sizeof(char) );
        
        // check if any field is null
        int varcharsize = -1;
        int offset = sizeof(char) + INT_SIZE;
        for(int i=0; i<3; i++)
        {
            if(!rbfm->fieldIsNull(nullField, i) )
            {
                switch(i)
                {
                    case 0:
                    {
                        memcpy(&varcharsize, (char*)data +offset, VARCHAR_LENGTH_SIZE);
                        offset += VARCHAR_LENGTH_SIZE;
                        char *name = (char*) malloc(varcharsize);
                        memcpy(name, (char*)data + offset, varcharsize);
                        offset += varcharsize;
                        attr.name.assign(name, varcharsize);
                        free(name);
                        break;
                    }
                    case 1:
                    {
                        AttrType t = *( (AttrType*) ( (char*)data + offset) );
                        offset += INT_SIZE;
                        attr.type = t;
                        break;
                    }
                    case 2:
                    {
                        unsigned l = *( (unsigned*) ( (char*)data + offset) );
                        offset += INT_SIZE;
                        attr.length = l;
                        break;
                    }
                }
            }
            // field is null
            else
            {
                switch(i)
                {
                    case 0:
                    {
                        attr.name = "";
                        break;
                    }
                    case 1:
                    {
                        attr.type = TypeInt;
                        break;
                    }
                    case 2:
                    {
                        attr.length = 15;
                        break;
                    }
                }
            }
        }
        free(nullField);
        attrs.push_back(attr);
    }
    rmsi.close();
    free(data);
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    string t_Name = tableName;
    
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    
    
    if (rbfm->openFile(t_Name, fileHandle) != 0)
    {
        return -1;
    }
    
    getAttributes(t_Name, recordDescriptor);
    
    if(rbfm->insertRecord(fileHandle, recordDescriptor, data, rid))
	return -1;
    if(rbfm->closeFile(fileHandle))
	return -1;
    return 0;
}


RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    string t_Name = tableName;
    // Make sure tableName is not a system table
    if (t_Name == "Tables" || t_Name == "Columns")
    {
        return -1;
    }
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    
    
    if (rbfm->openFile(t_Name, fileHandle) != 0)
    {
        return -1;
    }
    
    getAttributes(t_Name, recordDescriptor);
    if(rbfm->deleteRecord(fileHandle, recordDescriptor, rid))
	return -1;
    if(rbfm->closeFile(fileHandle))
	return -1;
    return 0;
    
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    string t_Name = tableName;
    // Make sure tableName is not a system table
    if (t_Name == "Tables" || t_Name == "Columns")
    {
        return -1;
    }
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    
    
    if (rbfm->openFile(t_Name, fileHandle) != 0)
    {
        return -1;
    }
    
    getAttributes(t_Name, recordDescriptor);
    
    if(rbfm->updateRecord(fileHandle, recordDescriptor, data, rid))
		return -1;
    if(rbfm->closeFile(fileHandle))
	return -1;
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    string t_Name = tableName;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    
    if (rbfm->openFile(t_Name, fileHandle) != 0)
    {
        return -1;
    }
    
    if(getAttributes(t_Name, recordDescriptor))
	return -1;
    if(rbfm->readRecord(fileHandle, recordDescriptor, rid, data))
	return -1;
    if(rbfm->closeFile(fileHandle))
	return -1;
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    string t_Name = tableName;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    
    
    if (rbfm->openFile(t_Name, fileHandle) != 0)
    {
        return -1;
    }
    
    getAttributes(t_Name, recordDescriptor);
    
    if(rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data))
	{
		cout<<"failed to read attribute!"<<endl;
		return -1;
	}
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if( rbfm->openFile(tableName, fileHandle))
		return -1;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	RBFM_ScanIterator rbfmsi;
	if(rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfmsi))
		return -1;
	rm_ScanIterator.rids = rbfmsi.rids;
	rm_ScanIterator.fileHandle = fileHandle;
	rm_ScanIterator.recordDescriptor = recordDescriptor;
	return 0;
}
