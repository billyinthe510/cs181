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
	
	// Check if file is open
	
	// Check if recordDescriptor prepared
	int fieldCount = recordDescriptor.size();
	if(fieldCount == 0 )
		return -1;
	
	// Find size of record to be inserted
	
	int nullbytes = ceil( (double) fieldCount / 8 );
	unsigned char *nullFieldsIndicator =  (unsigned char *) malloc(nullbytes);
	memcpy(nullFieldsIndicator, data, nullbytes);

	int fieldsRead = 0;
	int recordSize = 0;
	int offset = nullbytes;

	bool nullbit = false;
	for( int i=0; i<nullbytes; i++)
	{
		recordSize++;
		for( int j=7; j>=0; j--)
		{
			nullbit = nullFieldsIndicator[i] & ( 1 << j );
			if(!nullbit)
			{	
				// Add bytes if TypeVarChar
				Attribute attr = recordDescriptor[fieldsRead];
				switch(attr.type){
					case TypeInt:
					{	
						break;
					}
					case TypeReal:
					{	
						break;
					}
					case TypeVarChar:
					{
						char varLength = *((char*)data + offset);
						recordSize += (int)varLength;
						offset += varLength;
						break;
					}
					default:
						break;
				}
				recordSize += sizeof(int);
				offset += sizeof(int);
			}
			fieldsRead++;
			if(fieldsRead == fieldCount)
			{
				j=-1;
				i=nullbytes;
			}
		}
	}
	free(nullFieldsIndicator);

	// Find last page in file
	void *buffer = malloc(PAGE_SIZE);
	int *freeSpaceOffset = (int*) buffer + 1023;
	int *numSlots = (int*) buffer + 1022;

	int currentPage = fileHandle.getNumberOfPages()-1;
	if(currentPage == -1)
	{
		*(freeSpaceOffset) = 0;
		*(numSlots) = 0;
		fileHandle.appendPage(buffer);
	}
	currentPage = fileHandle.getNumberOfPages() - 1;
	// Find last slot in page
	fileHandle.readPage(currentPage, buffer);
	freeSpaceOffset = (int*) buffer + 1023;
	numSlots = (int*) buffer + 1022;

	// Check if free space is less than size of record + slot size
	if( *(numSlots) == 0)
	{
		rid.pageNum = currentPage;
		rid.slotNum = 1;
		// set 1st slot
		//SlotDr *setSlot;
		//setSlot = (SlotDr *) buffer + PAGE_SIZE - 16 ;
	//	setSlot->offset = 0;
	//	setSlot->length = recordSize;
		int *off = (int*) buffer + 1020;
		int *len = (int*) buffer + 1021;
		*(off) = 0;
		*(len) = recordSize;
		// update slot directory
		*(freeSpaceOffset) = recordSize;
		*(numSlots) = 1;
		
		memcpy( buffer, data, recordSize);
		fileHandle.writePage(currentPage, buffer);

		free(buffer);	
		return 0;
	}
	int freeSpace = PAGE_SIZE - 8 - *(freeSpaceOffset) - (*(numSlots)* 8 );
	if(freeSpace >= recordSize + 8 )
	{
		// set rid
		rid.pageNum = currentPage;
		rid.slotNum = *(numSlots)+1;
		// get last occupied slot's length
		//SlotDr *lastSlot = (SlotDr*) buffer + PAGE_SIZE - 8 - (*(numSlots)*sizeof(SlotDr) );
		int *lastRecordSize =  (int*) buffer + (1024 - 2 - (*(numSlots)*2) + 1);
		// set new slot
	//	SlotDr *setSlot;
	//	setSlot = (SlotDr*) buffer + PAGE_SIZE - 8 - ((*(numSlots)+1) * sizeof(SlotDr) );
	//	setSlot->offset = *(freeSpaceOffset) + lastRecordSize;
	//	setSlot->length = recordSize;
		int *off = (int*) buffer + (1022 - 2 - (*(numSlots)*2));
		int *len = (int*) buffer + (1022 - 2 - (*(numSlots)*2) +1);
		*(off) = *(freeSpaceOffset);
		*(len) = recordSize;
		*(freeSpaceOffset) = *(freeSpaceOffset) + recordSize;		
// copy record into buffer
		memcpy( (char*) buffer + *(freeSpaceOffset) - recordSize, data, recordSize);
		// update page slot directory's # of slots and free space pointer
		*(numSlots) = *(numSlots) + 1;
		// write buffer to page
		fileHandle.writePage(currentPage, buffer);
		free(buffer);
		return 0;
	}
	else
	{
		for(int k=0; k<currentPage; k++)
		{
			fileHandle.readPage(k,buffer);
			int *freeOffset = (int*) buffer + 1023;
			numSlots = (int*) buffer + 1022;
			freeSpace = PAGE_SIZE - 8 - *(freeOffset) - (*(numSlots)*8 );
			if(freeSpace >= recordSize + (int)sizeof(SlotDr) )
			{
				// set rid
				rid.pageNum = k;
				rid.slotNum = *(numSlots)+1;
				// get last occupied slot's length
				int lastRecordSize = *( (int*) buffer + 1024 - (*(numSlots)*2) + 1);
				// set new slot
				int *off = (int*) buffer + 1022 - 2 - (*(numSlots)*2);
				int *len = (int*) buffer + 1022 - 2 - (*(numSlots)*2) + 1;
				*(off) = *(freeOffset);
				*(len) = recordSize;
	*(freeOffset) += recordSize;
				// copy record into buffer
				memcpy( (char*)buffer + *(freeOffset) - recordSize, data, recordSize);
				// update page slot directory's # of slots and free space pointer
				*(numSlots) += 1;
	//			*(freeSpaceOffset) += recordSize;
				// write buffer to page
				fileHandle.writePage(k, buffer);
				free(buffer);
				buffer = NULL;
				return 0;
			}
		}
		// no free space in any previous pages
		free(buffer);
		buffer = NULL;
		void *buff = malloc(PAGE_SIZE);
		freeSpaceOffset = (int*) buff + 1023;
		numSlots = (int*) buff + 1022;
		currentPage = fileHandle.getNumberOfPages() - 1;
		// set rid
		rid.pageNum = currentPage+1;
		rid.slotNum = 1;
		// set 1st slot
		int *off = (int*) buff + 1020;
		int *len = (int*) buff + 1021;
		*(off) = 0;
		*(len) = recordSize;
		// update slot directory
		*(freeSpaceOffset) = recordSize;
		*(numSlots) = 1;
		// copy record to buff
		memcpy( buff, data, recordSize);
		// write buff to new page
		fileHandle.appendPage(buff);
		free(buff);
		return 0;	 
	}
	
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void *buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, buffer);
	int numSlots = *( (int*) buffer + 1022);
	int *recordOffset = (int*) buffer + 1022 - (rid.slotNum*2);
	int *recordSize = (int*) buffer + 1022 - (rid.slotNum*2) + 1;
memcpy(data, (char*)buffer + *(recordOffset), *(recordSize));
	free(buffer);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
   
   
	int fieldCount = recordDescriptor.size();
	if(fieldCount <= 0)
		return -1;
	int nullBytes = ceil((double) fieldCount / CHAR_BIT);
	int offset = nullBytes;
	
	bool nullBit = false;
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullBytes);
	memcpy( nullFieldsIndicator, data, nullBytes);
	
	int fieldsRead = 0;
	for(int i=0; i<nullBytes; i++)
	{
		for(int j=7; j>=0; j--)
		{
			cout<< recordDescriptor[fieldsRead].name<<": ";
	
			nullBit = nullFieldsIndicator[i] & (1 << j);
			if(!nullBit){
				Attribute attr = recordDescriptor[fieldsRead];
				switch(attr.type){
					case TypeInt:
					{
					 	//data = buffer[directoryOff + (sizeof int)]
                				void *print = malloc(sizeof (int));
                				memcpy(print,((char*) data)+offset, sizeof (int));
                				cout << *(int *) print << "   ";
                				free(print);
                				offset += sizeof (int);
						break;
					}
					case TypeReal:
					{
				       		void *print = malloc(sizeof (float));
                 				memcpy(print,((char*) data) +offset, sizeof (float));
                 				cout  << *(float *) print << "   ";
                 				free(print);
                 				offset += sizeof (float);
						break;
					}
					case TypeVarChar:
					{
						void *nameLength = malloc(sizeof(int));
						memcpy(nameLength, ((char*) data)+offset, sizeof(int));
						offset += sizeof(int);

						void *name = malloc( *(int*)nameLength );
						memcpy(name,((char*) data+offset), *(int*)nameLength );
						for(int k=0;k<*(int*)nameLength;k++)
							cout<<*((char*)name+k);
						cout<<"   ";
						offset += *(int*)nameLength;
						free(name);
						break;
					}
					default:
					{
						break;
					}
				}
			}
			else
				cout<<"NULL   ";
	
			fieldsRead++;
			if(fieldsRead==fieldCount)
			{
				free(nullFieldsIndicator);
				return 0;
			}
		}	 
	}	 

    return -1;
}

