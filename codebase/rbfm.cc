#include "rbfm.h"
#include "pfm.h"
#include <string.h>
#include <cstdio>
#include <math.h>
#include <iostream>
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    
    PagedFileManager *pfmanager = PagedFileManager::instance();
    return pfmanager->createFile(fileName);
    
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager *pfmanager = PagedFileManager::instance();
    return pfmanager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager *pfmanager = PagedFileManager::instance();
    return pfmanager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager *pfmanager = PagedFileManager::instance();
    return pfmanager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *buffer[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, buffer);
    int directoryOff = (rid.slotNum + 1) * 8; //move to begining of slot
    directoryOff = PAGE_SIZE - directoryOff; //move to begining of slot
    SlotDr *directory;
    directory = (SlotDr*) buffer[directoryOff];
    memcpy(data, buffer[directory->offset], directory->length);
   /* int off = 0;
    for(unsigned i = 0; i < recordDescriptor.size(); i++) {
        switch(recordDescriptor[i].type) {
            case TypeInt:
                //data = buffer[directoryOff + (sizeof int)]
                memcpy(data, buffer[directory->offset + off], sizeof (int));
                off += sizeof (int);
                break;
            case TypeReal:
                 //data = buffer[directoryOff + (sizeof float)]
                 memcpy(data, buffer[directory->offset + off], sizeof (float));
                 off += sizeof (float);
                 break;
            case TypeVarChar:
                //data = (buffer[directoryOff + (sizeof int)]
                memcpy(data, buffer[directory->offset + off], sizeof (int));
                void *length = malloc(sizeof (int));
                memcpy(length, buffer[directory->offset + off], sizeof (int));
                off += sizeof (int);  
                memcpy(data, buffer[directory->offset + off], *(int*)length);
                free(length);	 
                break;
        }
    }*/
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int off = ceil((double) recordDescriptor.size() / 8);
    for(unsigned i = 0; i < recordDescriptor.size(); i++) {
        switch(recordDescriptor[i].type) {
            case TypeInt: {
                //data = buffer[directoryOff + (sizeof int)]
                void *print = malloc(sizeof (int));
                memcpy(print, data + off, sizeof (int));
                cout << "TYPE INT: "<< *(int *) print << "\n";
                free(print);
                off += sizeof (int);
                break;
            }
            case TypeReal: {
                 //data = buffer[directoryOff + (sizeof float)]
                 void *print = malloc(sizeof (float));
                 memcpy(print, data + off, sizeof (float));
                 cout << "TYPEREAL: " << *(float *) print << "\n";
                 free(print);
                 off += sizeof (float);
                 break;
            }
            case TypeVarChar: {
                //data = (buffer[directoryOff + (sizeof int)]
                void *print = malloc(30);
                void *length = malloc(sizeof (int));
                memcpy(length, data + off, sizeof (int));
                
                cout << "TYPEVARCHAR LENGTH: "<< *(int *) length << "\n";
                off += sizeof (int);  
                memcpy(print, data + off, *(int*)length);
                cout << (char *) print << "\n";
                off += *(int*) length;
                free(length);	
                free(print); 
                break;
            }
        }
    }
    return 0;
}
