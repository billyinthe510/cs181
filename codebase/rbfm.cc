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

cout<<"rSize: "<<recordSize<<endl;
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
	
		currentPage++;
	}

	// Find last slot in page
	fileHandle.readPage(currentPage, buffer);
	freeSpaceOffset = (int*) buffer + 1023;
	numSlots = (int*) buffer + 1022;

cout<<"numSlots: "<<*(numSlots)<<endl<<"freeSpaceOffset: "<<*(freeSpaceOffset)<<endl;

	// Check if free space is less than size of record + slot size
	if( *(numSlots) == 0)
	{
		rid.pageNum = currentPage;
		rid.slotNum = 1;
		// set 1st slot
		SlotDr *setSlot;
		setSlot = ((SlotDr *) buffer + PAGE_SIZE - 16 );
		setSlot->offset = 0;
		setSlot->length = recordSize;
		// update slot directory
		*(freeSpaceOffset) = recordSize;
		*(numSlots) = 1;
cout<<"recordSize: "<<recordSize<<endl;
		memcpy( buffer, data, recordSize);
		fileHandle.writePage(currentPage, buffer);

int freeOff = *( (int*)buffer + 1023 );
cout<<"freeOff: "<<freeOff<<endl<<"\n";
		free(buffer);
cout<<endl;	return 0;
	}
	int freeSpace = PAGE_SIZE - *(freeSpaceOffset) - (*(numSlots)* sizeof(SlotDr) );
	if(freeSpace >= recordSize + (int) sizeof(SlotDr) )
	{
		// set rid
		rid.pageNum = currentPage;
		rid.slotNum = *(numSlots)+1;
		// get last occupied slot's length
		SlotDr *lastSlot = (SlotDr*) buffer + PAGE_SIZE - 8 - (*(numSlots)*sizeof(SlotDr) );
		int lastRecordSize = lastSlot->length;
		// set new slot
cout<<"lastRecordSize: "<<lastRecordSize<<endl;
		SlotDr *setSlot;
		setSlot = (SlotDr*) buffer + PAGE_SIZE - 8 - ((*(numSlots)+1) * sizeof(SlotDr) );
		setSlot->offset = *(freeSpaceOffset) + lastRecordSize;
		setSlot->length = recordSize;
cout<<"recordSize: "<<recordSize<<endl;		
		// update page slot directory's # of slots and free space pointer
		*(numSlots) += 1;
		*(freeSpaceOffset) += recordSize;
		// copy record into buffer
		memcpy( ((char*)buffer) + *(freeSpaceOffset) + lastRecordSize , data, recordSize);
		// write buffer to page
		fileHandle.writePage(currentPage, buffer);
		free(buffer);
cout<<endl;		return 0;
	}
	else
		cout<<"not enough space in this page \n\n";
	
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void *buffer = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, buffer);
	SlotDr *recordSlot =   (SlotDr*)buffer + PAGE_SIZE - sizeof(int) - sizeof(int) - (rid.slotNum*sizeof(SlotDr)) ;
	int recordOffset = recordSlot->offset;
	int recordSize = recordSlot->length;
	memcpy(data, (char*)buffer + recordOffset, recordSize);
/*
unsigned char * nullindicator = (unsigned char*)malloc(sizeof(char));
memcpy(nullindicator,data,sizeof(char));
for(int i=7;i>=0;i--)
{
	bool  nullbit = nullindicator[0] & (1 << i);
cout<<nullbit<<endl;
}
	printRecord(recordDescriptor, data);	
*/

/*
    SlotDr *directory;
    directory = (SlotDr*) buffer[directoryOff];
    memcpy(data, buffer[directory->offset], directory->length);
   
    int off = 0;
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
				cout<<"NULL\t";
	
			fieldsRead++;
			if(fieldsRead==fieldCount)
				return 0;
		}	 
	}	 

    return -1;
}

