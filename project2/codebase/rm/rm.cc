
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
    // Table
    vector<Attribute> table_Data;
    
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
    // Check if "Tables" exists already
    struct stat stFileInfo;
    if (stat(fileName1.c_str(), &stFileInfo) != 0 )
        return -1;
    // Check if "Columns" exists already
    if (stat(fileName2.c_str(), &stFileInfo) != 0)
        return -1;
    
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm->destroyFile(fileName1);
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
    
    for (int i = 0; i < attrs.size(); i++)
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
        rbfm->printRecord(column_Data, test1);
        free(col_Data);
    }
    free(nullsIndicator);	
	
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	struct stat stFileInfo;
	if (stat(tableName.c_str(), &stFileInfo) == 0)
        return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	//rbfm->openFile("Columns");
	for(int i = 0; i < attrs.size(); i++) {
	//	rbfm->readAttribute()
	}
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}
