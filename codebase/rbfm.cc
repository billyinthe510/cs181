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
    void *buffer = malloc(PAGE_SIZE);
    void *work = malloc(100);
    rid.pageNum = fileHandle.getNumberOfPages();
    cout << "PAGE NUM " << rid.pageNum;
    fileHandle.readPage(rid.pageNum, buffer);
    int size = ceil((double) recordDescriptor.size() / 8);
    //printRecord(recordDescriptor, work + size);
    for(unsigned i = 0; i < recordDescriptor.size(); i++) {
        switch(recordDescriptor[i].type) {
            case TypeInt: {
                
                memcpy(work + size, data + size, sizeof(int));
                size += sizeof (int);
                cout << size << "\n";
                break;
            }
            case TypeReal: {
                 
                 memcpy(work + size, data + size, sizeof(float));
                 size += sizeof (float);
                 cout << size << "\n";
                 break;
            }
            case TypeVarChar: {
                //data = (buffer[directoryOff + (sizeof int)]
                void *length = malloc(sizeof (int));
                memcpy(length, data + size, sizeof (int));
                memcpy(work + size,length, sizeof (int));
                size = size + sizeof (int);
                cout << "Length " << *(int*)length;
                cout << size << "\n";
                memcpy(work + size, data + size, *(int*)length);
                size += *(int*)length;
                free(length);	 
                break;
            }
        }
    }
    printRecord(recordDescriptor, data);
    int slotOffset = 0;
    int recordOffset = 0;
    SlotDr *directoryOff = new SlotDr();
    SlotDr *test = new SlotDr();
    //test->offset = 0;
    //test->length = 0;
    //cout << sizeof(SlotDr) << "\n";
    memcpy(directoryOff, buffer + PAGE_SIZE - sizeof(SlotDr), sizeof(SlotDr));
    /*for(directoryOff; directoryOff->next != NULL; directoryOff = directoryOff->next) {
         slotOffset++;
         cout << "\nhello\n" << "\nOFFSET: " << directoryOff->offset; ;
         recordOffset += directoryOff->offset;
         
    }*/
    int newSlot = slotOffset * sizeof(SlotDr) + sizeof(SlotDr);
    rid.slotNum = slotOffset;
    SlotDr *directory = (SlotDr *)malloc(sizeof(SlotDr));
    directory->length = size;
    directory->offset = recordOffset;
    int freeSpace = PAGE_SIZE - recordOffset + (slotOffset * sizeof(SlotDr));
    cout << "FREE SPACE " << freeSpace << "\n Directory offset " << directory->offset;
    cout << "\n Slot Directory " << directory->length <<"\n";
    //memcpy(buffer + PAGE_SIZE - newSlot, &directory, sizeof(SlotDr));
    if(freeSpace < size + sizeof(SlotDr)) {
        fileHandle.appendPage(data);
        //fileHandle.readPage(rid.pageNum++, );
    }
    else {
        cout << "BUFFER b4\n" << directory->length;
        //memcpy((buffer + directory->offset), work, directory->length + 1);
        //printRecord(recordDescriptor, work + size);
        //fwrite(buffer, 1, PAGE_SIZE, fileHandle.getFP());
        cout << "BUFFER " << (int *)buffer;
        //memcpy((buffer + PAGE_SIZE - newSlot), &directory, sizeof(SlotDr));
        //int help = fwrite(buffer, 1, PAGE_SIZE, fileHandle.getFP());
        //cout << "HELLO I NEED HELP " << help;
        //memcpy(work, buffer, 25);
        //printRecord(recordDescriptor, work + size);
        //printRecord(recordDescriptor, buffer);
        //memcpy(buffer + (PAGE_SIZE - newSlot), directory->length, sizeof(directory->length));
        //printRecord(recordDescriptor, work + size);
        int off = ceil((double) recordDescriptor.size() / 8);
        for(unsigned i = 0; i < recordDescriptor.size(); i++) {
        switch(recordDescriptor[i].type) {
            case TypeInt: {
                
                memcpy(buffer + off, work + off, sizeof(int));
                off += sizeof (int);
                cout << off << "\n";
                break;
            }
            case TypeReal: {
                 
                 memcpy(buffer + off, work + off, sizeof(float));
                 off += sizeof (float);
                 cout << off << "\n";
                 break;
            }
            case TypeVarChar: {
                //data = (buffer[directoryOff + (sizeof int)]
                void *length = malloc(sizeof (int));
                memcpy(length, work + off, sizeof (int));
                memcpy(buffer + off,length, sizeof (int));
                off = off + sizeof (int);
                cout << "Length \n" << *(int*)length;
                cout << off << "\n";
                memcpy(buffer + off, work + off, *(int*)length);
                off += *(int*)length;
                free(length);	 
                break;
            }
        }
        
    }
        memcpy(buffer + PAGE_SIZE - newSlot, directory, sizeof(SlotDr));
        //cout << *(unsigned*) (buffer + PAGE_SIZE - newSlot) << "\n TESTING: " << *(unsigned*) buffer + PAGE_SIZE - 4;
        fwrite(buffer, 1, PAGE_SIZE, fileHandle.getFP());
        //printRecord(recordDescriptor, buffer);
        //void* testing = malloc(500);
        //memcpy(testing, buffer + directory->offset + 1, sizeof(int));
        //cout << "TEST: " << *(int *) testing; 
        //printRecord(recordDescriptor, buffer);
    }
    
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *buffer = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, buffer);
    int directoryOff = (rid.slotNum + 1) * sizeof(SlotDr); //move to begining of slot
    directoryOff = PAGE_SIZE - directoryOff; //move to begining of slot
    SlotDr *directory;
    memcpy(directory, buffer + directoryOff, sizeof(SlotDr));
    cout << "READ: OFF " << directory->offset << "\n RECORD LENGTH: " << directory->length << "\n";
    //memcpy(data, buffer + directory->offset, directory->length);
    int off = ceil((double) recordDescriptor.size() / 8);
    for(unsigned i = 0; i < recordDescriptor.size(); i++) {
        switch(recordDescriptor[i].type) {
            case TypeInt:
                //data = buffer[directoryOff + (sizeof int)]
                memcpy(data + directory->offset + off, buffer + directory->offset + off, sizeof (int));
                off += sizeof (int);
                cout << off << "\n";
                break;
            case TypeReal:
                 //data = buffer[directoryOff + (sizeof float)]
                 memcpy(data + directory->offset + off, buffer + directory->offset + off, sizeof (float));
                 off += sizeof (float);
                 cout << off << "\n";
                 break;
            case TypeVarChar:
                //data = (buffer[directoryOff + (sizeof int)]
                memcpy(data + directory->offset + off, buffer + directory->offset + off, sizeof (int));
                void *length = malloc(sizeof (int));
                memcpy(length, buffer + directory->offset + off, sizeof (int));
                off += sizeof (int);  
                memcpy(data + directory->offset + off, buffer + directory->offset + off, *(int*)length);
                off += *(int*) length;
                cout << off << "\n";
                free(length);	 
                break;
        }
    }
    cout << "READ RECORD SIZE: " << off << "\n";
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
               
                void *length = malloc(sizeof (int));
                memcpy(length, data + off, sizeof (int));
                
                cout << "TYPEVARCHAR LENGTH: "<< *(int *) length << "\n";
                off += sizeof (int);  
                void *print = malloc(*(int *) length);
                memcpy(print, data + off, *(int*)length);
                for(int i = 0; i < *(int *) length; i++)
                    cout << *(char *) (print + i);
                cout << "\n";
                off += *(int*) length;
                free(length);	
                free(print); 
                break;
            }
        }
    }
    return 0;
}
