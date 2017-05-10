
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
    string fileName1("Tables");
    string fileName2("Columns");
    
    
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
    memcpy(table, &t_ID, sizeof(int));
    t_Offset += sizeof(int);
    // table-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)table + t_Offset, &fileName1, t_nameLen);
    t_Offset += t_nameLen;
    // file-name
    memcpy((char *)table + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)table + t_Offset, &fileName1, t_nameLen);
    t_Offset += t_nameLen;
    // table-type
    memcpy((char *)table + t_Offset, &t_typeLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)table + t_Offset, t_Type.c_str(), t_typeLen);
    t_Offset +=  t_typeLen;
    
    rbfm->insertRecord(fileHandle, table_Data, table, rid);
    //rbfm->closeFile(fileHandle);
    free(table);
    
    void *column = malloc(tableLen + (3 * sizeof(int)));;
    //unsigned t2_Offset = 0;
    t_ID = 2;
    t_nameLen = fileName2.length();
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
    memcpy(column, &t_ID, sizeof(int));
    t_Offset += sizeof(int);
    // table-name
    memcpy((char *)column + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)column + t_Offset, &fileName2, t_nameLen);
    t_Offset += t_nameLen;
    // file-name
    memcpy((char *)column + t_Offset, &t_nameLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)column + t_Offset, &fileName2, t_nameLen);
    t_Offset += t_nameLen;
    // table-type
    memcpy((char *)column + t_Offset, &t_typeLen, sizeof(int));
    t_Offset += sizeof(int);
    memcpy((char*)column + t_Offset, &t_Type, t_typeLen);
    t_Offset +=  t_typeLen;
    
    rbfm->insertRecord(fileHandle, table_Data, column, rid);
    rbfm->closeFile(fileHandle);
    free(column);
    
    // Check if "Columns" exists already
    if (stat(fileName2.c_str(), &stFileInfo) == 0)
        return -1;
    
    // Create "Columns"
    rbfm->createFile(fileName2);
    rbfm->openFile(fileName2, fileHandle);
    
    // Populate
    unsigned offset = 0;
    for (int i = 0; i < 8; i++)
    {
        void *col_Data = malloc(columnLen + sizeof(int));
        unsigned int tab_ID;
        string col_Name;
        unsigned int col_Type;
        unsigned int col_Len;
        unsigned int col_Pos;
        
        nullFieldsIndicatorActualSize = ceil((double) table_Data.size() / CHAR_BIT);
        
        // Null-indicator for the fields
        nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
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
                break;
            }
            case 1: // (1, "table-name", TypeVarChar, 50, 2)
            {
                tab_ID = 1;
                col_Name = "table-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 2;
                break;
            }
            case 2: // (1, "file-name", TypeVarChar, 50, 3)
            {
                tab_ID = 1;
                col_Name = "file-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 3;
                break;
            }
            case 3: // (2, "table-id", TypeInt, 4, 1)
            {
                tab_ID = 2;
                col_Name = "table-id";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 1;
                break;
            }
            case 4: // (2, "column-name", TypeVarChar, 50, 2)
            {
                tab_ID = 2;
                col_Name = "column-name";
                col_Type = TypeVarChar;
                col_Len = AttrLength(50);
                col_Pos = 2;
                break;
            }
            case 5: // (2, "column-type", TypeInt, 4, 3)
            {
                tab_ID = 2;
                col_Name = "column-type";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 3;
                break;
            }
            case 6: // (2, "column-length", TypeInt, 4, 4)
            {
                tab_ID = 2;
                col_Name = "column-length";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 4;
                break;
            }
            case 7: // (2, "column-position", TypeInt, 4, 5)
            {
                tab_ID = 2;
                col_Name = "column-position";
                col_Type = TypeInt;
                col_Len = AttrLength(4);
                col_Pos = 5;
                break;
            }
        }
        memcpy(col_Data, &tab_ID, sizeof(int));
        offset += sizeof(int);
        
        memcpy(col_Data, &(col_Name), sizeof(int));
        offset += sizeof(int);
        memcpy((char*)col_Data + offset,  &col_Name, col_Name.length());
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
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    string fileName1 = "Tables";
    string fileName2 = "Columns";
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
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
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
