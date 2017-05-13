#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    //cout << "SLOTHEADER OFFSET: " << slotHeader.freeSpaceOffset << endl << "RECORD SIZE: " << recordSize << endl;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

	// Record is forwarded or deleted
	if(!isAlive(recordEntry))
	{
		if( recordEntry.length ==0 && recordEntry.offset ==0)
		{
			// Record has been deleted
			free(pageData);
			return -1;
		}
		else
		{
			// Record is forwarded
			RID forwardRID;
			forwardRID.pageNum = recordEntry.length;
			forwardRID.slotNum = -1*recordEntry.offset;
			readRecord(fileHandle,recordDescriptor,forwardRID,data);
		}
	}
	else
	{
	    // Retrieve the actual entry data
	    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
	}
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    int rc;
    // Retrieve the specific page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    
    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
    // Alive
    if (recordEntry.offset > 0)
    {
        //slotHeader.freeSpaceOffset += recordEntry.offset;
        recordEntry.offset = 0;
        recordEntry.length = 0;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        //setSlotDirectoryHeader(pageData, slotHeader);
        
        compact(fileHandle, rid, pageData);
        rc = 0;
        
    }
    // Moved
    else if (recordEntry.offset < 0)
    {
        RID rid2;
        rid2.pageNum = recordEntry.length;
        rid2.slotNum = -1 * (rid.slotNum);
        rc = deleteRecord(fileHandle, recordDescriptor, rid2);
    }
    // Dead
    else
    {
        rc = -1;
    }
    free(pageData);
    return rc;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute>
&recordDescriptor,const void *data, const RID &rid)
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Read the page
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if(fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    //Check slot and make sure it is valid.
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
	if(slotHeader.recordEntriesNumber < rid.slotNum)
	{
		free(pageData);
		return RBFM_SLOT_DN_EXIST;
	}
  	else
	{
        SlotDirectoryRecordEntry slotEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
        //Entry is dead
        if(slotEntry.offset == 0) {
            free(pageData);
            return RBFM_SLOT_DN_EXIST;
        }
        //Entry has relocated
        else if(slotEntry.offset < 0) {
            //slotEntry should have link to where the data is
            RID forwardRid;
            forwardRid.pageNum = slotEntry.length;
            forwardRid.slotNum = abs(slotEntry.offset);
            
            updateRecord(fileHandle, recordDescriptor, data, forwardRid);
        }
        //Entry is alive
        else {
            //Entry is same size
            if(recordSize == slotEntry.length) {
                setRecordAtOffset (pageData, slotEntry.offset, recordDescriptor, data);
                fileHandle.writePage(rid.pageNum, pageData);
            }
            //Entry has shrunk
            else if(recordSize < slotEntry.length) {
                //maybe update slotEntry.offset 
                slotEntry.length = recordSize;
                setSlotDirectoryRecordEntry(pageData, rid.slotNum, slotEntry);
                
                setRecordAtOffset (pageData, slotEntry.offset, recordDescriptor, data);
                compact(fileHandle, rid, pageData);
                fileHandle.writePage(rid.pageNum, pageData);
            }
            //Entry is larger
            else {
                //There is enough free space for the record in the page.
                //+slotEntry.length because deleting previous version of the record
                if(getPageFreeSpaceSize(pageData) + slotEntry.length >= recordSize) {
                    //delete first? then write to page?
                    
                    deleteRecord(fileHandle, recordDescriptor, rid);
                    //update slot directory
                    slotEntry.length = recordSize;
                    slotEntry.offset = slotHeader.freeSpaceOffset - recordSize;
                    setSlotDirectoryRecordEntry(pageData, rid.slotNum, slotEntry);
                    //update SlotHeader
                    slotHeader.freeSpaceOffset = slotEntry.offset;
                    setSlotDirectoryHeader(pageData, slotHeader);
                    //add data to page
                    setRecordAtOffset (pageData, slotEntry.offset, recordDescriptor, data);
                    fileHandle.writePage(rid.pageNum, pageData);
                }
                //Not enough free space for the record in the page
                else {
                    compact(fileHandle, rid, pageData);
                    //check free space again
                    //+slotEntry.length because deleting previous version of the record
                    if(getPageFreeSpaceSize(pageData) + slotEntry.length >= recordSize) {
                        deleteRecord(fileHandle, recordDescriptor, rid);
                        //update slot directory
                        slotEntry.length = recordSize;
                        slotEntry.offset = slotHeader.freeSpaceOffset - recordSize;
                        setSlotDirectoryRecordEntry(pageData, rid.slotNum, slotEntry);
                        //update SlotHeader
                        slotHeader.freeSpaceOffset = slotEntry.offset;
                        setSlotDirectoryHeader(pageData, slotHeader);
                        //add data to page
                        setRecordAtOffset (pageData, slotEntry.offset, recordDescriptor, data);
                        fileHandle.writePage(rid.pageNum, pageData);
                    }
                    //Not enough free space for the record in the page
                    else {
                        //delete record. then insert to a different page update slotEntry.offset,length 
                        deleteRecord(fileHandle, recordDescriptor, rid);
                        //insert record somewhere else
                        RID forwardRid;
                        insertRecord(fileHandle, recordDescriptor, data, forwardRid);
                        //update slotEntry to be a forwarding address to where the record is now
                        slotEntry.length = forwardRid.pageNum;
                        //negative because forwarding
                        slotEntry.offset = 0 - forwardRid.slotNum;
                        setSlotDirectoryRecordEntry(pageData, rid.slotNum, slotEntry);
                        fileHandle.writePage(rid.pageNum, pageData);
                    }
                }
            }
            
        }
    }
    free(pageData);
    return SUCCESS;
    
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
const RID &rid, const string &attributeName, void *data)
{
	// Find the page
	void *pageData = malloc(PAGE_SIZE);
	if(pageData == NULL)
	{
		free(pageData);
		return RBFM_MALLOC_FAILED;
	}
	if(fileHandle.readPage(rid.pageNum,pageData))
	{
		free(pageData);
		return RBFM_READ_FAILED;
	}
	// Find the slot header and validate
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
	if(slotHeader.recordEntriesNumber < rid.slotNum)
	{
		free(pageData);
		return RBFM_SLOT_DN_EXIST;
	}
	// Get the slot
	SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData,rid.slotNum);
	// Get index from vector<Attributes>
	int descriptorIndex = -1;
	unsigned i;
	AttrType type;
	for(i=0; i<recordDescriptor.size(); i++)
	{
		if(recordDescriptor[i].name == attributeName)
		{
			descriptorIndex = (int)i;
			i = recordDescriptor.size();
			type = recordDescriptor[i].type;
		}
	}
	if(descriptorIndex == -1)
	{
		// Error if attributeName is not found in recordDescriptor
		free(pageData);
		return -1;
	}
	// Get the record
	void *record = malloc(PAGE_SIZE);
	if( readRecord(fileHandle, recordDescriptor,rid, record) != SUCCESS )
	{
		// Record has been deleted
		free(pageData);
		free(record);
		return -1;
	}

	// Get nullbits
	int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
	char nullIndicator[nullIndicatorSize];
	memset(nullIndicator, 0, nullIndicatorSize);
	memcpy(nullIndicator, (char*)record + sizeof(RecordLength), nullIndicatorSize);
	// Check null value
	char nullIndicate;
	if(fieldIsNull(nullIndicator, descriptorIndex))
	{
		nullIndicate = ( 1 << 7);
		memcpy(data, nullIndicate, sizeof(char) );
		free(pageData);
		free(record);
		return SUCCESS;
	}
	// Find field in null index
	int nullIndex = 0;
	int nIndex = 0;
	int aliveIndex = 0;
	for(i=0; i<recordDescriptor.size(); i++)
	{
		if( !fieldIsNull(nullIndicator,i) )
		{
			if(i == (unsigned)descriptorIndex)
				nullIndex = nIndex;
			nIndex++;
			aliveIndex++;
		}
	}
	// Set nullIndicator in returnedData
	memset(nullIndicate, 0, sizeof(char) );
	memcpy(data, nullIndicate, sizeof(char) );
	
	// Get field offset and data
	void *offset = malloc(sizeof(ColumnOffset)*2);
	ColumnOffset fieldOffset;
	if(nullIndex == 0)
	{
		memcpy(offset, (char*)record + sizeof(RecordLength) + nullIndicatorSize, sizeof(ColumnOffset) );
		fieldOffset = *( (ColumnOffset*) offset);
		int fieldStartOffset = sizeof(RecordLength) +  nullIndicatorSize + (sizeof(ColumnOffset)*aliveIndex);
		memcpy(data+sizeof(char), (char*)record + fieldStartOffset, fieldOffset - fieldStartOffset);
	}
	else
	{
		memcpy(offset, (char*)record + sizeof(RecordLength) + nullIndicatorSize + (sizeof(ColumnOffset)*(nullIndex-1), (sizeof(ColumnOffset)*2) );
		int prevFieldOffset = *( (ColumnOffset*) offset);
		fieldOffset = *( (ColumnOffset*) offset + 1);
		memcpy(data+sizeof(char), (char*)record + prevFieldOffset, fieldOffset - prevFieldOffset);
	}
	free(pageData);
	free(offset);
	free(record);
	return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
const string &conditionAttribute, const CompOp compOp, const void *value,
const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
    return -1;
}
// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

RC RecordBasedFileManager::isAlive(SlotDirectoryRecordEntry slotEntry)
{
	return !( (slotEntry.length ==0 && slotEntry.offset==0)|| slotEntry.offset<0 );
}

void RecordBasedFileManager::compact(FileHandle &fileHandle, const RID &rid, void *page)
{
	// Get slot header
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
	unsigned slotsOccupied = (unsigned) slotHeader.recordEntriesNumber;

	// Gather alive slots
	SlotDirectoryRecordEntry currentSlot;
	unsigned recordSpace = 0;
	void *records = malloc(PAGE_SIZE);

	for(unsigned i = slotsOccupied-1; i >= 0; i--)
	{
		// Get current slot
		currentSlot = getSlotDirectoryRecordEntry(page,i);
		// Skip if slot is dead or forwarded
		if( !isAlive(currentSlot) )
			;
		// Slot is alive
		else
		{
			memcpy( records, (char *)page+currentSlot.offset, currentSlot.length);
			recordSpace += currentSlot.length;
		}
	}

	// Update slot entry offsets
	unsigned freeSpace = PAGE_SIZE;
	for( unsigned j = 0; j < slotsOccupied; j++)
	{
		currentSlot = getSlotDirectoryRecordEntry(page,j);
		if( !isAlive(currentSlot) )
			;
		else
		{
			freeSpace -= currentSlot.length;
			// Update current slot entry
			currentSlot.offset = freeSpace;
			setSlotDirectoryRecordEntry(page, j, currentSlot);
		}
	}
	// Copy records back into page
	memcpy( (char *)page+freeSpace, records, recordSpace);
	// Update Slot Header freeSpaceOffset
	slotHeader.freeSpaceOffset = freeSpace;
	setSlotDirectoryHeader(page, slotHeader);
	
	// Write compacted records to disk
	fileHandle.writePage(rid.pageNum, page);

	free(records);	
}
