
#include "ix.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>



IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	// Creating a new paged file
   	string ixFile = fileName + ".ix";
	if(pfm->createFile(ixFile))
		return IX_CREATE_FAILED;

	// Setting up the first page
	void *firstPageData = malloc(PAGE_SIZE);
	if(firstPageData == NULL)
		return IX_MALLOC_FAILED;
	newIndexBasedPage(firstPageData);

	// Add p0 pointer to page 1
	uint32_t p0 = 1;
	memcpy( ((char*)firstPageData + sizeof(IndexHeader)), &p0, INT_SIZE );

	// Adds the first index based page
	FileHandle fileHandle;
	
	if (pfm->openFile(ixFile.c_str(), fileHandle) )
	{	
		free(firstPageData);
		return IX_OPEN_FAILED;
	}
	
	if(fileHandle.appendPage(firstPageData))
	{
		free(firstPageData);
		return IX_APPEND_FAILED;
	}
	
	// Add empty leaf page pointed to by p0
	free(firstPageData);
	void *firstLeafPage = calloc(PAGE_SIZE, 1);
	if(firstLeafPage == NULL)
		return IX_MALLOC_FAILED;
	newIndexBasedPage(firstLeafPage);

	// Initialize header of empty leaf page
	IndexHeader ixHeader;
	ixHeader = getIndexHeader(firstLeafPage);
	ixHeader.isLeafPage = true;
		// For leaf pages, subtract the p0 pointer from freespace
	ixHeader.freeSpaceOffset -= INT_SIZE;
	ixHeader.parent = 0;
	setIndexHeader(firstLeafPage, ixHeader);
	if(fileHandle.appendPage(firstLeafPage))
	{
		free(firstLeafPage);
		return IX_APPEND_FAILED;
	}

	pfm->closeFile(fileHandle);
	free(firstLeafPage);
	return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
        string ixFile = fileName + ".ix";
	if( !isIndexFile(ixFile))
	    return IX_DESTROY_FAILED;
	return pfm->destroyFile(ixFile);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
        string ixFile = fileName + ".ix";
	// Validate index file
	if( !isIndexFile(ixFile))
		return IX_OPEN_FAILED;
	
	// If this handle already has an open file, error
	if( ixfileHandle.getfd() != NULL)
		return PFM_HANDLE_IN_USE;
	// If the file doesn't exist, error
	if( !fileExists(ixFile.c_str()))
		return PFM_FILE_DN_EXIST;
	// Open the file for reading/writing in binary mode
	FILE *pFile;
	pFile = fopen(ixFile.c_str(),"rb+");
	// If failed to open, error
	if( pFile == NULL)
		return IX_OPEN_FAILED;
	ixfileHandle.setfd(pFile);

	return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	//if( !isIndexFile(fileName))
	//	return IX_CLOSE_FAILED;
	FILE *pFile = ixfileHandle.getfd();

	// If not an open file, error
	if( pFile == NULL)
		return 1;
	// Flush and close the file
	fclose(pFile);	
	ixfileHandle.setfd(NULL);

	return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // validate that FILE is set
    if(ixfileHandle.getfd() == NULL)
	return IX_NULL_FILE;
    // Get Root Node (page 0)
    void *pageData = malloc(PAGE_SIZE);
    if(pageData == NULL)
	return IX_MALLOC_FAILED;

    RC rc;
    rc = ixfileHandle.readPage(0, pageData);
    if( rc != SUCCESS)
    {
	// Failed to read page 0
  	free(pageData);
	return rc;
    }
    // Get index header
    IndexHeader ixHeader;
    ixHeader = getIndexHeader(pageData);

    // If indexEntriesNmber is 0 then tree must be empty
    if( ixHeader.indexEntriesNumber == 0)
	// helper function frees pageData for us
	return insertIntoEmptyTree(ixfileHandle, attribute, key, rid, pageData);

    // Else follow "traffic cops" 
    //     until leaf page is found
    //     pass in page0 for fewer I/Os
    uint32_t leafPage = findLeafPage(ixfileHandle, attribute, key, pageData);
    if( leafPage == 0)
    {
	// Could not find a leaf page must be logic error somewhere
	free(pageData);
	return POINTER_ERROR;
    }
    // get leaf page 
    rc = ixfileHandle.readPage(leafPage, pageData);
    if( rc != SUCCESS)
    {
	free(pageData);
	return rc;
    }
    // insert data into leaf page
    // helper function will free pageData
    if( insertIntoLeafPage(ixfileHandle, attribute, key, rid, leafPage, pageData))
	return BAD_INPUT;

    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute)
{
	// get the root
	void *pageData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(0, pageData);
	Attribute myAttr = attribute;
	printNonLeaf(ixfileHandle, 0, 0, myAttr);
	
}
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
	_fd = NULL;
	attr_type = TypeNull;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    // If pageNum doesn't exist, error
    if (getNumberOfPages() < pageNum)
	return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
	return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
	return FH_READ_FAILED;

    ixReadPageCounter++;
    return SUCCESS;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    // Check if the page exists
    if (getNumberOfPages() < pageNum)
	return FH_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
	return FH_SEEK_FAILED;
	
    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
	// Immediately commit changes to disk
	fflush(_fd);
	ixWritePageCounter++;
	return SUCCESS;
    }

    return FH_WRITE_FAILED;
}

RC IXFileHandle::appendPage(const void *data)
{
    // Seek to the end of the file
    if (fseek(_fd, 0, SEEK_END))
	return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
	fflush(_fd);
	ixAppendPageCounter++;
      	return SUCCESS;
    }
    return FH_WRITE_FAILED;
}
unsigned IXFileHandle::getNumberOfPages()
{
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
	// On error, return 0
	return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

void IXFileHandle::setfd(FILE *fd)
{
	_fd = fd;
}
FILE *IXFileHandle::getfd()
{
	return _fd;
}
void IXFileHandle::setAttr(AttrType type)
{
	attr_type = type;
}
AttrType IXFileHandle::getAttr()
{
	return attr_type;
}
// HELPER FUNCTIONS
bool IndexManager::isIndexFile(const string &fileName)
{
	return  strcmp( fileName.substr(fileName.size()-3).c_str() , ".ix") == 0;
}

bool IndexManager::fileExists(const string &fileName)
{
    // If stat fails, we can safely assume the file doesn't exist
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}
void IndexManager::setIndexHeader(void *data, IndexHeader ixHeader) 
{
    memcpy (data, &ixHeader, sizeof(IndexHeader));
}
IndexHeader IndexManager::getIndexHeader(void *data)
{
    return *((IndexHeader*) data);
}
void IndexManager::newIndexBasedPage(void* data) 
{
    memset(data, 0, PAGE_SIZE);
    // Writes the slot directory header.
    IndexHeader ixHeader;
    ixHeader.freeSpaceOffset = sizeof(IndexHeader) + INT_SIZE;
    ixHeader.indexEntriesNumber = 0;
    ixHeader.isLeafPage = false;
    ixHeader.parent = -1;
    ixHeader.left = -1;
    ixHeader.right = -1;
    setIndexHeader(data, ixHeader);
}

RC IndexManager::insertIntoEmptyTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *pageData)
{
    // Get index header
    IndexHeader ixHeader;
    ixHeader = getIndexHeader(pageData);
    uint16_t freeSpace = ixHeader.freeSpaceOffset;
	
	// Set index file's attribute type
	ixfileHandle.setAttr(attribute.type);

	// add key value to root node
	NonLeafEntry newEntry;
	newEntry.child = 2;
	switch(attribute.type)
	{
		case TypeInt:
		{
			newEntry.size = INT_SIZE;
			memcpy( (char*)pageData + freeSpace, &newEntry, sizeof(NonLeafEntry));
			freeSpace += sizeof(NonLeafEntry);

			memcpy( (char*)pageData + freeSpace, key, INT_SIZE);
			freeSpace += INT_SIZE;
			break;
		}
		case TypeReal:
		{
			newEntry.size = REAL_SIZE;
			memcpy( (char*)pageData + freeSpace, &newEntry, sizeof(NonLeafEntry));
			freeSpace += sizeof(NonLeafEntry);

			memcpy( (char*)pageData + freeSpace, key, REAL_SIZE);
			freeSpace += REAL_SIZE;
			break;
		}
		case TypeVarChar:
		{
			int varcharlen;
			memcpy(&varcharlen, key, INT_SIZE);
			newEntry.size = 2;
			memcpy( (char*)pageData + freeSpace, &newEntry, sizeof(NonLeafEntry));
			freeSpace += sizeof(NonLeafEntry);

			memcpy( (char*)pageData + freeSpace, (char*)key + INT_SIZE, VARCHAR_LENGTH_SIZE);
			freeSpace += VARCHAR_LENGTH_SIZE;
			break;
		}
		default:
		{
			break;
		}
	}
	// update header
	ixHeader.indexEntriesNumber = 1;
	ixHeader.freeSpaceOffset = freeSpace;
	setIndexHeader(pageData, ixHeader);
	// write root page to disk
	if( ixfileHandle.writePage(0, pageData))
	{
		free(pageData);
		return FH_WRITE_FAILED;
	}
	// prevent memory leak
	free(pageData);

	// set up a leaf page for first index entry
	void *leafPageData = malloc(PAGE_SIZE);
	if( leafPageData == NULL)
		return IX_MALLOC_FAILED;
		
	newIndexBasedPage(leafPageData);
	ixHeader = getIndexHeader(leafPageData);
	freeSpace = ixHeader.freeSpaceOffset;
	ixHeader.isLeafPage = true;
	// for leaf pages, remove the default p0 pointer from freeSpaceOffset
	freeSpace -= INT_SIZE;
	// add data entry key value to leaf node
	LeafEntry newDataEntry;
	newDataEntry.rid = rid;
	switch(attribute.type)
	{
		case TypeInt:
		{
			newDataEntry.size = INT_SIZE;
			memcpy( (char*)leafPageData + freeSpace, &newDataEntry, sizeof(LeafEntry));
			freeSpace += sizeof(LeafEntry);

			memcpy( (char*)leafPageData + freeSpace, key, INT_SIZE);
			freeSpace += INT_SIZE;
			break;
		}
		case TypeReal:
		{
			newDataEntry.size = REAL_SIZE;
			memcpy( (char*)leafPageData + freeSpace, &newDataEntry, sizeof(LeafEntry));
			freeSpace += sizeof(LeafEntry);

			memcpy( (char*)leafPageData + freeSpace, key, REAL_SIZE);
			freeSpace += REAL_SIZE;
			break;
		}
		case TypeVarChar:
		{
			int varlen;
			memcpy(&varlen, key, INT_SIZE);
			newDataEntry.size = varlen;
			memcpy( (char*)leafPageData + freeSpace, &newDataEntry, sizeof(LeafEntry));
			freeSpace += sizeof(LeafEntry);

			memcpy( (char*)leafPageData + freeSpace, (char*)key + INT_SIZE, varlen);
			freeSpace += varlen;
			break;
		}
		default:
		{
			break;
		}
	}
	// update leaf page's header
	ixHeader.freeSpaceOffset = freeSpace;
	ixHeader.parent = 0;
	ixHeader.left = 1;
	ixHeader.indexEntriesNumber = 1;
	setIndexHeader(leafPageData, ixHeader);
	// write leaf page to disk
	if( ixfileHandle.appendPage(leafPageData))
	{
		free(leafPageData);
		return IX_APPEND_FAILED;
	}
	// prevent memory leak
	free(leafPageData);

	// update p0's neighbor (right sibling)
	void *p0 = malloc(PAGE_SIZE);
	if( p0 == NULL)
		return  IX_MALLOC_FAILED;

	RC rc;
	rc = ixfileHandle.readPage(1, p0);
	if( rc != SUCCESS)
	{
		// failed to read page 1
		free(p0);
		return rc;
	}
	// get index header
	ixHeader = getIndexHeader(p0);
	// set right child of p0's page to newly appended page (could be 2 or could be different pageNum)
	ixHeader.right = 2;
	setIndexHeader(p0, ixHeader);
	// write p0's page 1 to disk
	if( ixfileHandle.writePage(1, p0))
	{
		free(p0);
		return FH_WRITE_FAILED;
	}
	// prevent memory leak
	free(p0);

	return SUCCESS;
}

uint32_t IndexManager::getNextChild(void* pageData, const Attribute &attribute, const void* key)
{
    // Read the root page header
    IndexHeader rootHeader;
    memcpy(&rootHeader, pageData, sizeof(IndexHeader));
    // Get the number of entries
    int numEntries = rootHeader.indexEntriesNumber;
    // Read through the non-leaf entry until found suitable page pointer
    int offset = sizeof(IndexHeader) + INT_SIZE;
    uint32_t p0;
    memcpy(&p0, (char*) pageData + sizeof(IndexHeader),  INT_SIZE);

    NonLeafEntry prevEntry;	
    int prevSize = 0;
    for(int i = 0; i < numEntries; i++) 
    {
        // get the next entry
        NonLeafEntry entry;
        memcpy(&entry, (char*)pageData + offset, sizeof(NonLeafEntry));
	offset += sizeof(NonLeafEntry);
        // read the value in the entry. Compare with key
        switch(attribute.type) 
        {
            case(TypeInt):
	    {
	            int nextInt;
	            int keyInt;
	            memcpy(&keyInt, key, INT_SIZE);
	            memcpy(&nextInt, ((char*)pageData + offset), INT_SIZE);
		    offset += INT_SIZE;
	
	            // return p0 if key is less than first search key value            
	            if(i == 0 && keyInt < nextInt)
	             	return p0;
		    // return previous key's child pointer if key is less than current search key value
	            if( keyInt < nextInt)
		    {
		        // get prevEntry's child
			memcpy(&prevEntry, ((char*)pageData + offset - (2*INT_SIZE) - (2*sizeof(NonLeafEntry))), sizeof(NonLeafEntry));
			return prevEntry.child;	
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry
	            if(i == numEntries - 1 && keyInt >= nextInt)
			return entry.child;

            	    break; 
	    }
            case(TypeReal):
	    { 
		    float nextReal;
	            float keyReal;
	            memcpy(&keyReal, key, sizeof(int));
	            memcpy(&nextReal, ((char*) pageData + offset), REAL_SIZE);
	            offset += REAL_SIZE;
				
		    // return p0 if key is less than first search key value
	            if(i == 0 && keyReal < nextReal)
	            	return p0;
		    // return previous key's child pointer if key is less than current search key value	   
	            if(keyReal < nextReal)
	            {
			// get prevEntry's child
	                memcpy(&prevEntry, (char*)pageData + offset - (2*REAL_SIZE) - (2*sizeof(NonLeafEntry)), sizeof(NonLeafEntry));
	                return prevEntry.child;
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry
	            if(i == numEntries - 1 && keyReal >= nextReal)
	              	return entry.child;
					
	            break;
            }
	    case(TypeVarChar):
	    { 
		    // get key value
		    int length;
	            memcpy(&length, (char*)key, INT_SIZE);
	            char* keyVarChar = (char*) malloc(length);
	            memcpy(keyVarChar, (char*)key + INT_SIZE, length);
	            // get current search key value
		    int nextLength = entry.size;
	            char* nextVarChar = (char*)malloc(nextLength);  
	            memcpy(nextVarChar, (char*) pageData + offset, nextLength);
	            offset += nextLength;

		    // return p0 if key is less than first search key value
	            if(i == 0 && strcmp(keyVarChar, nextVarChar) < 0)
	              	return p0;
		    // return previous key's child poiter if key is less than current search key value
		    // guaranteed to be not p0 (first page pointer)
	            if(strcmp(keyVarChar ,nextVarChar) < 0)
	            {
	                memcpy(&prevEntry, (char*)pageData + offset - nextLength - (2*sizeof(NonLeafEntry)) - prevSize, sizeof(NonLeafEntry));
	                return prevEntry.child;
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry   
	            if(i == numEntries - 1 && strcmp(keyVarChar, nextVarChar) >= 0)
	    		return entry.child;
					
		    // keep track of current length
	            prevSize = nextLength;    
	            free(keyVarChar);
	            free(nextVarChar);            
	            break;
	    }
	    default:
	    {
		   break;
	    }
        }
    } 
    // traversed non leaf entries without finding an appropriate pointer
    return ERROR_FINDING_LEAF_PAGE;
}

uint32_t IndexManager::findLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *pageData)
{
	// get header for root page
	IndexHeader ixHeader = getIndexHeader(pageData);
	// follow non-leaf pages until proper leaf page is found
	uint32_t pagePtr = 0;
	while(ixHeader.isLeafPage != true)
	{
		pagePtr = getNextChild(pageData, attribute, key);
		if(pagePtr == ERROR_FINDING_LEAF_PAGE)
			return 0;
		ixfileHandle.readPage(pagePtr, pageData);
		ixHeader = getIndexHeader(pageData);	 
	}
	return pagePtr;
}

RC IndexManager::insertIntoLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, uint32_t &leafPage, void *pageData)
{
    // get the key value
    int size;
    void *keyVal = malloc(PAGE_SIZE);
    if( keyVal == NULL)
	return IX_MALLOC_FAILED;

    switch(attribute.type)
    {
	case TypeInt:
	{
	    size = INT_SIZE;
	    memcpy( keyVal, key, INT_SIZE);
	    break;
	}
	case TypeReal:
	{
	    size = REAL_SIZE;
	    memcpy( keyVal, key, REAL_SIZE);
	    break;
	}
	case TypeVarChar:
	{
	    memcpy( &size, key, VARCHAR_LENGTH_SIZE);
	    memcpy( keyVal, (char*)key + VARCHAR_LENGTH_SIZE, size);
	    break;
	}
	default:
	    break;
    }
 
    // get index header
    IndexHeader ixHeader = getIndexHeader(pageData);

    int offset = sizeof(IndexHeader);
    LeafEntry dataEntry;
    dataEntry.rid = rid;
    // base case: leaf page is empty
    if( ixHeader.indexEntriesNumber == 0)
    {
	switch(attribute.type)
	{
	    case TypeInt:
	    {
		dataEntry.size = INT_SIZE;
		memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
		offset += sizeof(LeafEntry);

		memcpy( (char*)pageData + offset, (char*)keyVal, INT_SIZE);
		offset += INT_SIZE;
		break;
	    }
	    case TypeReal:
	    {
		dataEntry.size = REAL_SIZE;
		memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
		offset += sizeof(LeafEntry);

		memcpy( (char*)pageData + offset, (char*)keyVal, REAL_SIZE);
		offset += REAL_SIZE;
		break;
	    }
	    case TypeVarChar:
	    {
		int varlen;
	   	memcpy(&varlen, (char*)key, VARCHAR_LENGTH_SIZE);
		dataEntry.size = varlen;
		if(ixHeader.freeSpaceOffset < sizeof(LeafEntry) + varlen)
		{
		    // a single varchar entry should be less than PAGE_SIZE
		    free(pageData);
		    return BAD_INPUT;
		}
		memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
		offset += sizeof(LeafEntry);

		memcpy( (char*)pageData + offset, (char*) keyVal + VARCHAR_LENGTH_SIZE, varlen);
		offset += varlen;
	  	break;
	     }
	     default:
		break;
	}
	// update index header
	ixHeader.freeSpaceOffset = offset;
	ixHeader.indexEntriesNumber = 1;
	setIndexHeader(pageData, ixHeader);
	if( ixfileHandle.writePage(leafPage, pageData))
	{
		free(keyVal);
		free(pageData);
		return FH_WRITE_FAILED;
	}
	free(keyVal);
	free(pageData);
	return SUCCESS;
    }
		
    // ideal case: insert entry into leaf without splitting
    if( (unsigned)(sizeof(LeafEntry)+size+ixHeader.freeSpaceOffset) <= (unsigned)(PAGE_SIZE) )
    {
	int numEntries = ixHeader.indexEntriesNumber;
	bool inserted = false;
	for(int i = 0; i < numEntries; i++) 
	{
	        // get the next entry
	        LeafEntry entry;
	        memcpy(&entry, (char*)pageData + offset, sizeof(LeafEntry));
	        offset += sizeof(LeafEntry);
	        // read the value in the entry. Compare with key and insert data entry
	        switch(attribute.type) 
	        {
	            case(TypeInt):
		    {
			dataEntry.size = INT_SIZE;
					
		        int nextInt;
		        int keyInt;
		        memcpy(&keyInt, key, INT_SIZE);
		        memcpy(&nextInt, ((char*)pageData + offset), INT_SIZE);
			offset += INT_SIZE;
					         
		        if(i == 0 && keyInt < nextInt)
		        {
				// memmov all entries if key is less than first search key value   
				memmove( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry) + INT_SIZE, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
				memcpy( (char*)pageData + sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry), keyVal, INT_SIZE);
				inserted = true;
			}
		        else if( keyInt < nextInt)
			{
				// memmove all entries at current entry if key is less than current search key value
			        memmove( (char*)pageData + offset, (char*)pageData + offset - INT_SIZE - sizeof(LeafEntry), ixHeader.freeSpaceOffset - offset + sizeof(LeafEntry) + INT_SIZE);
				memcpy( (char*)pageData + offset - INT_SIZE - sizeof(LeafEntry), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset - INT_SIZE, keyVal, INT_SIZE);
				inserted = true;
		        }
		        else if(i == numEntries - 1 && keyInt >= nextInt)
			{
				// insert to end if key is greater than or equal to last search key value entry
				memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset + sizeof(LeafEntry), keyVal, INT_SIZE);
				inserted = true;
			}
					
			if(inserted)
			{
				ixHeader.freeSpaceOffset += sizeof(LeafEntry) + INT_SIZE;
				ixHeader.indexEntriesNumber += 1;
				setIndexHeader(pageData, ixHeader);
				if(ixfileHandle.writePage(leafPage, pageData))
				{
					free(pageData);
					free(keyVal);
					return IX_MALLOC_FAILED;
				}
				free(pageData);
				free(keyVal);
				return SUCCESS;
			}
	            	break; 
		    }
	            case(TypeReal):
		    {
			dataEntry.size = REAL_SIZE;
					
			float nextReal;
		        float keyReal;
		        memcpy(&keyReal, key, REAL_SIZE);
		        memcpy(&nextReal, ((char*) pageData + offset), REAL_SIZE);
		        offset += REAL_SIZE;
					
			if(i == 0 && keyReal < nextReal)
		        {
				// memmov all entries if key is less than first search key value   
				memmove( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry) + REAL_SIZE, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
				memcpy( (char*)pageData + sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry), keyVal, REAL_SIZE);
				inserted = true;
			}
	       		else if( keyReal < nextReal)
			{
				// memmove all entries at current entry if key is less than current search key value
			        memmove( (char*)pageData + offset, (char*)pageData + offset - REAL_SIZE - sizeof(LeafEntry), ixHeader.freeSpaceOffset - offset + sizeof(LeafEntry) + REAL_SIZE);
				memcpy( (char*)pageData + offset - REAL_SIZE - sizeof(LeafEntry), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset - REAL_SIZE, keyVal, REAL_SIZE);
				inserted = true;
		        }
		        else if(i == numEntries - 1 && keyReal >= nextReal)
			{
				// insert to end if key is greater than or equal to last search key value entry
				memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset + sizeof(LeafEntry), keyVal, REAL_SIZE);
				inserted = true;
			}
					
			if(inserted)
			{
				ixHeader.freeSpaceOffset += sizeof(LeafEntry) + REAL_SIZE;
				ixHeader.indexEntriesNumber += 1;
				setIndexHeader(pageData, ixHeader);
				if(ixfileHandle.writePage(leafPage, pageData))
				{
					free(pageData);
					free(keyVal);
					return IX_MALLOC_FAILED;
				}
				free(pageData);
				free(keyVal);
				return SUCCESS;
			}
				
		        break;
	            }
		    case(TypeVarChar):
		    { 
			 // get key value
			 int length;
			 memcpy(&length, (char*)key, INT_SIZE);
			 char* keyVarChar = (char*) malloc(length);
			 memcpy(keyVarChar, (char*)key + INT_SIZE, length);
					
			 dataEntry.size = length;
					
			 // get current search key value
			int nextLength = entry.size;
			char* nextVarChar = (char*)malloc(nextLength);  
			memcpy(nextVarChar, (char*) pageData + offset, nextLength);
			offset += nextLength;
		
		        if(i == 0 && strcmp(keyVarChar, nextVarChar) < 0)
		        {
				// memmov all entries if key is less than first search key value   
				memmove( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry) + length, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
				memcpy( (char*)pageData + sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry), keyVarChar, length);
				inserted = true;
			}
		        else if( strcmp(keyVarChar, nextVarChar) < 0)
			{
				// memmove all entries at current entry if key is less than current search key value
			        memmove( (char*)pageData + offset - entry.size + length, (char*)pageData + offset - entry.size - sizeof(LeafEntry), ixHeader.freeSpaceOffset - offset + sizeof(LeafEntry) + entry.size);
				memcpy( (char*)pageData + offset - entry.size - sizeof(LeafEntry), &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset - entry.size, keyVarChar, length);
				inserted = true;
		        }
		        else if(i == numEntries - 1 && strcmp(keyVarChar, nextVarChar) >= 0)
			{
				// insert to end if key is greater than or equal to last search key value entry
				memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
				memcpy( (char*)pageData + offset + sizeof(LeafEntry), keyVarChar, length);
				inserted = true;
			}
			// prevent memory leaks
			free(keyVarChar);
			free(nextVarChar);
			
			if(inserted)
			{
				ixHeader.freeSpaceOffset += sizeof(LeafEntry) + length;
				ixHeader.indexEntriesNumber += 1;
				setIndexHeader(pageData, ixHeader);
				if(ixfileHandle.writePage(leafPage, pageData))
				{		
					free(pageData);
					free(pageData);
					return IX_MALLOC_FAILED;
				}
				free(pageData);
				free(keyVal);
				return SUCCESS;
			}           
			break;
		    }
		    default:
			 break;
	         }
	}
	free(pageData);
	free(keyVal);
	return BAD_INPUT;
    }

    // else split
    if( split(ixfileHandle, attribute, rid, leafPage, pageData, size, keyVal))
    {
	free(keyVal);
	return BAD_INPUT;
    }
    free(keyVal);
    return SUCCESS;
}
bool IndexManager::getIsLeafPage(IXFileHandle &ixfileHandle, uint32_t pageNum)
{
	void *pageData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, pageData);
	IndexHeader head;
	memcpy(&head, pageData, sizeof(IndexHeader));
	free(pageData);
	return head.isLeafPage;
}
void IndexManager::printNonLeaf(IXFileHandle &ixfileHandle, uint32_t pageNum, int numSpaces, const Attribute &attribute) 
{
    void* pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, pageData);
    //Read the root page header
    IndexHeader pageHeader;
    memcpy(&pageHeader, pageData, sizeof(IndexHeader));
    //Get the number of entries
    int numEntries = pageHeader.indexEntriesNumber;
    //Read through the non-leaf entry until found suitable place
    //offset = size of all of the previous void* data
    uint32_t firstPage;
    memcpy(&firstPage,(char*) pageData + sizeof(IndexHeader),  sizeof(int));
    int offset = 0;
    char spaces[numSpaces + 1];
    
    for(int i = 0; i < numSpaces; i++)
        spaces[i] = ' ';
    spaces[numSpaces] = '\0';
    cout << spaces << "\"keys\":[";
    
    //Print out each key in this page
    for(int i = 0; i < numEntries; i++)
    {
        NonLeafEntry entry;
        memcpy(&entry, (char*)pageData + sizeof(IndexHeader) + (i * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(NonLeafEntry));
        if(entry.size >= 0)
        {
            switch(attribute.type) 
            {
                case(TypeInt):
                int nextInt;
                memcpy(&nextInt, (char*)pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(int));  
        
                offset += sizeof(int);
                if(i == 0)
                    cout << "\"" << nextInt << "\"";
                else 
                    cout << "\", \"" << nextInt;
                
                break; 
                case(TypeReal):
                float nextReal;
        
                memcpy(&nextReal,(char*) pageData + + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(int));
                offset += sizeof(float);
                if(i == 0)
                    cout << "\"" << nextReal << "\":";
                else
                    cout << "\", \"" << nextInt;
                
                break;
                case(TypeVarChar):
                int nextLength = entry.size;
                char* nextVarChar = (char*)malloc(nextLength + 1);
        
                memcpy(nextVarChar, (char*) pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(int));
                nextVarChar[nextLength] = '\0';
                offset += nextLength;
                if (i == 0)
                    cout << "\"" << nextVarChar << "\",";    
                else
                    cout << "\", \"" << nextVarChar;
                break;
            }
        }
    }
    cout << "]," << endl;
    cout << spaces << "{\"Children\": [" << endl;
    offset = 0;
    for(int i = 0; i < numEntries; i++) 
    {
        //get the next entry
        NonLeafEntry entry;
        memcpy(&entry, (char*)pageData + sizeof(IndexHeader) + (i * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(NonLeafEntry));
        //Read the value in the entry. Compare with key
        switch(attribute.type) 
        {
            case(TypeInt):
            int nextInt;
            memcpy(&nextInt, (char*)pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(int));  
            offset += sizeof(int);
            
                if(i == 0)
                {
                    if(getIsLeafPage(ixfileHandle, firstPage) == false)
                    {
                        printNonLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute); 
                        printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                    }
                    else
                    {
                        printLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute);
                        printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                    }
                
               }
               else 
               {
                    if(getIsLeafPage(ixfileHandle, firstPage) == false)
                    {
                        printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                    }
                    else
                    { 
                        printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                    }
                }
            
        break; 
        case(TypeReal):
        float nextReal;
        
        memcpy(&nextReal,(char*) pageData + + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset + sizeof(int), sizeof(int));
        offset += sizeof(float);
       
            if(i == 0)
            {
                if(getIsLeafPage(ixfileHandle, firstPage) == false)
                {
                    printNonLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute); 
                    printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                }
                else
                {
                    printLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute);
                    printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                }
            
           }
           else 
           {
                if(getIsLeafPage(ixfileHandle, firstPage) == false)
                {
                    printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                }
                else
                { 
                    printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                }
            }
        
        break;
        case(TypeVarChar):
        
        int nextLength = entry.size;
        char* nextVarChar = (char*)malloc(nextLength);
        
        memcpy(nextVarChar, (char*) pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(NonLeafEntry)) + offset, sizeof(int));
        offset += nextLength;
            if(i == 0)
            {
                if(getIsLeafPage(ixfileHandle, firstPage) == false)
                {
                    printNonLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute); 
                    printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                }
                else
                {
                    printLeaf(ixfileHandle, firstPage, numSpaces + 4, attribute);
                    printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                }
            
           }
           else 
           {
                if(getIsLeafPage(ixfileHandle, firstPage) == false)
                {
                    printNonLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);
                }
                else
                { 
                    printLeaf(ixfileHandle, entry.child, numSpaces + 4, attribute);                    
                }
            }
        break;
        }
    } 
}
void IndexManager::printLeaf(IXFileHandle &ixfileHandle, uint32_t pageNum, int numSpaces, const Attribute &attribute) 
{
    //Read the root page header
    //void* rootHeader = malloc(sizeof(IndexHeader));
    void* pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, pageData);
    IndexHeader rootHeader;
    rootHeader = getIndexHeader(pageData);
   // memcpy(&rootHeader, pageData, sizeof(IndexHeader));
    //Get the number of entries
    int numEntries = rootHeader.indexEntriesNumber;
    //Read through the non-leaf entry until found suitable place
    //offset = size of all of the previous void* data
    int offset = 0;
    int prevInt = -1;
    int prevReal = -1;
    char* prevVarChar;
    char spaces[numSpaces + 1];
    for(int i = 0; i < numSpaces; i++)
        spaces[i] = ' ';
    spaces[numSpaces] = '\0';
    
    if(numEntries > 0)
    {
        cout << spaces << "{\"keys\": [\"";
        for(int i = 0; i < numEntries; i++) 
        {
            //Read the value in the entry. Compare with key
            LeafEntry leafEntry;
            memcpy(&leafEntry, (char*)pageData + sizeof(IndexHeader) + ((i) * sizeof(LeafEntry)) + offset, sizeof(LeafEntry));  
            switch(attribute.type) 
            {
                
                case(TypeInt):
                int nextInt;
                //get the data
                memcpy(&nextInt, (char*)pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(LeafEntry)) + offset, sizeof(int));  
                if(leafEntry.size >= 0)
                {
                    if(prevInt == -1)
                    {
                        cout << nextInt << ":[(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                    else if(prevInt == nextInt)
                    {
                        cout << ",(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";   
                    } 
                    else if(prevInt != nextInt)
                    {
                        cout << "]\",\"" << nextInt << ":[" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                }
                prevInt = nextInt;
                offset += sizeof(int);
                break; 
                case(TypeReal):
                float nextReal;
                memcpy(&nextReal,(char*) pageData + + sizeof(IndexHeader) + ((i + 1) * sizeof(LeafEntry)) + offset, sizeof(int));
            
                if(leafEntry.size >= 0)
                {
                    if(prevReal == -1)
                    {
                        cout << nextReal << ":[(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                    if(prevReal == nextReal)
                    {
                        cout << ",(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";   
                    } 
                    else if(prevReal != nextReal)
                    {
                        cout << "]\",\"" << nextReal << ":[" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                }
                offset += sizeof(float);
                break;
                case(TypeVarChar):
                int nextLength = leafEntry.size;
                char* nextVarChar = (char*)malloc(nextLength);
            
                memcpy(nextVarChar, (char*) pageData + sizeof(IndexHeader) + ((i + 1) * sizeof(LeafEntry)) + offset , nextLength);
            
                if(leafEntry.size >= 0)
                {
                    if(prevVarChar == NULL)
                    {
                        cout << nextVarChar << ":[(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                    if(strcmp(prevVarChar, nextVarChar) == 0)
                    {
                        cout << ",(" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";   
                    } 
                    else if(strcmp(prevVarChar, nextVarChar) != 0)
                    {
                        cout << "]\",\"" << nextVarChar << ":[" << leafEntry.rid.pageNum << "," << leafEntry.rid.slotNum << ")";
                    }
                }
                offset += nextLength;        
                break;
            }
        } 
        cout << "]\"]}," << endl;
    }
    free(pageData);
}

RC IndexManager::split(IXFileHandle &ixfileHandle, const Attribute &attribute, const RID &rid, uint32_t leafPage, void *pageData, int size, void *keyVal)
{/*
	// malloc space to copy entries in order
	void *copy = malloc(2*PAGE_SIZE);
	if( copy == NULL)
	{
		free(pageData);
		return IX_MALLOC_FAILED;
	}

	// get page header
	IndexHeader ixHeader = getIndexHeader(pageData);
	int numEntries = ixHeader.indexEntriesNumber;

	if(ixHeader.isLeafPage)
	{
		int offset = sizeof(IndexHeader);
		LeafEntry dataEntry;
		dataEntry.rid = rid;
		// copy entries to get the correct order
		bool inserted = false;
		for(int i = 0; i < numEntries; i++) 
		{
		        // get the next entry
		      	LeafEntry entry;
		        memcpy(&entry, (char*)pageData + offset, sizeof(LeafEntry));
		        offset += sizeof(LeafEntry);
		        // read the value in the entry. Compare with key and insert data entry
		        switch(attribute.type) 
		        {
		            case(TypeInt):
			    	{
						dataEntry.size = INT_SIZE;
						
			        	int nextInt;
			        	int keyInt;
			        	memcpy(&keyInt, key, INT_SIZE);
			        	memcpy(&nextInt, ((char*)pageData + offset), INT_SIZE);
						offset += INT_SIZE;
						         
			        	if(i == 0 && keyInt < nextInt)
			        	{
							// copy entry then rest of entries if key is less than first search key value
							memcpy( copy, &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + sizeof(LeafEntry), keyVal, INT_SIZE);
							memcpy( (char*)copy + sizeof(LeafEntry) + INT_SIZE, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							inserted = true;
						}
			        	else if( keyInt < nextInt)
						{
							// copy all entries up to current entry if key is less than current search key value
							// copy entry to be inserted
							// copy all remaining entries
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), offset-sizeof(IndexHeader) - sizeof(LeafEntry) - INT_SIZE;
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - sizeof(LeafEntry) - INT_SIZE, &dataEntry, sizeof(LeafEntry);
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - INT_SIZE, keyVal, INT_SIZE);
							memcpy( (char*)copy + offset - sizeof(IndexHeader), (char*)pageData + offset - sizeof(LeafEntry) - INT_SIZE, ixHeader.freeSpaceOffset-offset+sizeof(LeafEntry)+INT_SIZE);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && keyInt >= nextInt)
						{
							// copy all entries
							// then copy entry to be inserted
							// if key is greater than or equal to last search key value entry
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader)+sizeof(LeafEntry), keyVal, INT_SIZE);
							inserted = true;
						}
						
						if(inserted)
						{
							// copy half of the entries to leafPage
							int half = floor((double)(numEntries+1)/2);
							memcpy( (char*)pageData + sizeof(IndexHeader), (char*)copy, half*(sizeof(LeafEntry) + INT_SIZE));
							int oldRightSibling = ixHeader.right;
							ixHeader.right = ixfileHandle.getNumberOfPages();
							ixHeader.indexEntriesNumber = half;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (half*(sizeof(LeafEntry)+INT_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.writePage(leafPage, pageData)
							{
								free(pageData);
								free(copy);
								return FH_WRITE_FAILED;
							}
							// append numEntries+1- half to new page
							int remaining = numEntries+1-half;
							memcpy( (char*)pageData+sizeof(IndexHeader), (char*)copy+ (half*(sizeof(LeafEntry)+INT_SIZE)),remaining*(sizeof(LeafEntry)+INT_SIZE));
								// get middle entry to copy up
								void *middle = malloc(INT_SIZE);
								memcpy( &middle, (char*)copy + (half*(sizeof(LeafEntry)+INT_SIZE)) + sizeof(LeafEntry), INT_SIZE);
								 
							ixHeader.left = leafPage;
							ixHeader.right = oldRightSibling;
							ixHeader.indexEntriesNumber = remaining;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (remaining*(sizeof(LeafEntry)+INT_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.appendPage(pageData))
							{
								free(pageData);
								free(copy);
								free(middle);
								return IX_APPEND_FAILED;
							}
							free(copy);
							// update oldRightSibling's left ptr
							if( oldRightSibling == -1)
								continue;
							else
							{
								if(ixfileHandle.readPage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_READ_FAILED;
								}
								ixHeader = getIndexHeader(pageData);
								ixHeader.left = ixfileHeader.getNumberOfPages()-1;
								setIndexHeader(pageData);
								if(ixfileHandle.writePage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_WRITE_FAILED;
								}
							}
							// copy up middle entry to non-leaf node
							if(ixfileHandle.readPage(ixHeader.parent, pageData))
							{
								free(pageData);
								free(middle);
								return FH_READ_FAILED;	  
							}
							if(split(ixfileHandle, attribute, rid, ixfileHandle.getNumberOfPages()-1, pageData, size, middle))
							{
								free(pageData);
								free(middle);
								return BAD_INPUT;
							}
							free(pageData);
							free(middle);
							return SUCCESS;
						}
		            	break; 
			    	}
		            case(TypeReal):
			    	{
						dataEntry.size = REAL_SIZE;
						
			        	float nextReal;
			        	float keyReal;
			        	memcpy(&keyReal, key, REAL_SIZE);
			        	memcpy(&nextReal, ((char*)pageData + offset), REAL_SIZE);
						offset += REAL_SIZE;
						         
			        	if(i == 0 && keyReal < nextReal)
			        	{
							// copy entry then rest of entries if key is less than first search key value
							memcpy( copy, &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + sizeof(LeafEntry), keyVal, REAL_SIZE);
							memcpy( (char*)copy + sizeof(LeafEntry) + REAL_SIZE, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							inserted = true;
						}
			        	else if( keyReal < nextReal)
						{
							// copy all entries up to current entry if key is less than current search key value
							// copy entry to be inserted
							// copy all remaining entries
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), offset-sizeof(IndexHeader) - sizeof(LeafEntry) - REAL_SIZE;
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - sizeof(LeafEntry) - REAL_SIZE, &dataEntry, sizeof(LeafEntry);
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - REAL_SIZE, keyVal, REAL_SIZE);
							memcpy( (char*)copy + offset - sizeof(IndexHeader), (char*)pageData + offset - sizeof(LeafEntry) - REAL_SIZE, ixHeader.freeSpaceOffset-offset+sizeof(LeafEntry)+INT_SIZE);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && keyReal >= nextReal)
						{
							// copy all entries
							// then copy entry to be inserted
							// if key is greater than or equal to last search key value entry
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader)+sizeof(LeafEntry), keyVal, REAL_SIZE);
							inserted = true;
						}
						
						if(inserted)
						{
							// copy half of the entries to leafPage
							int half = floor((double)(numEntries+1)/2);
							memcpy( (char*)pageData + sizeof(IndexHeader), (char*)copy, half*(sizeof(LeafEntry) + REAL_SIZE));
							int oldRightSibling = ixHeader.right;
							ixHeader.right = ixfileHandle.getNumberOfPages();
							ixHeader.indexEntriesNumber = half;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (half*(sizeof(LeafEntry)+REAL_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.writePage(leafPage, pageData)
							{
								free(pageData);
								free(copy);
								return FH_WRITE_FAILED;
							}
							// append numEntries+1- half to new page
							int remaining = numEntries+1-half;
							memcpy( (char*)pageData+sizeof(IndexHeader), (char*)copy+ (half*(sizeof(LeafEntry)+REAL_SIZE)),remaining*(sizeof(LeafEntry)+REAL_SIZE));
								// get middle entry to copy up
								void *middle = malloc(REAL_SIZE);
								memcpy( &middle, (char*)copy + (half*(sizeof(LeafEntry)+REAL_SIZE)) + sizeof(LeafEntry), REAL_SIZE);
								 
							ixHeader.left = leafPage;
							ixHeader.right = oldRightSibling;
							ixHeader.indexEntriesNumber = remaining;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (remaining*(sizeof(LeafEntry)+REAL_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.appendPage(pageData))
							{
								free(pageData);
								free(copy);
								free(middle);
								return IX_APPEND_FAILED;
							}
							free(copy);
							// update oldRightSibling's left ptr
							if( oldRightSibling == -1)
								continue;
							else
							{
								if(ixfileHandle.readPage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_READ_FAILED;
								}
								ixHeader = getIndexHeader(pageData);
								ixHeader.left = ixfileHeader.getNumberOfPages()-1;
								setIndexHeader(pageData);
								if(ixfileHandle.writePage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_WRITE_FAILED;
								}
							}
							// copy up middle entry to non-leaf node
							if(ixfileHandle.readPage(ixHeader.parent, pageData))
							{
								free(pageData);
								free(middle);
								return FH_READ_FAILED;	  
							}
							if(split(ixfileHandle, attribute, rid, ixfileHandle.getNumberOfPages()-1, pageData, size, middle))
							{
								free(pageData);
								free(middle);
								return BAD_INPUT;
							}
							free(pageData);
							free(middle);
							return SUCCESS;
						}
		            	break; 
		            }
			    	case(TypeVarChar):
			    	{ 
						 // get key value
						 int length;
						 memcpy(&length, (char*)key, INT_SIZE);
						 char* keyVarChar = (char*) malloc(length);
						 memcpy(keyVarChar, (char*)key + INT_SIZE, length);
								
						 dataEntry.size = length;
								
						 // get current search key value
						int nextLength = entry.size;
						char* nextVarChar = (char*)malloc(nextLength);  
						memcpy(nextVarChar, (char*) pageData + offset, nextLength);
						offset += nextLength;
					
					    if(i == 0 && strcmp(keyVarChar, nextVarChar) < 0)
					    {
							// memmov all entries if key is less than first search key value   
							memmove( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry) + length, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							memcpy( (char*)pageData + sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry), keyVarChar, length);
							inserted = true;
						}
				        else if( strcmp(keyVarChar, nextVarChar) < 0)
						{
							// memmove all entries at current entry if key is less than current search key value
				        	memmove( (char*)pageData + offset - entry.size + length, (char*)pageData + offset - entry.size - sizeof(LeafEntry), ixHeader.freeSpaceOffset - offset + sizeof(LeafEntry) + entry.size);
							memcpy( (char*)pageData + offset - entry.size - sizeof(LeafEntry), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + offset - entry.size, keyVarChar, length);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && strcmp(keyVarChar, nextVarChar) >= 0)
						{
							// insert to end if key is greater than or equal to last search key value entry
							memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + offset + sizeof(LeafEntry), keyVarChar, length);
							inserted = true;
						}
						// prevent memory leaks
						free(keyVarChar);
						free(nextVarChar);
						
						if(inserted)
						{
							ixHeader.freeSpaceOffset += sizeof(LeafEntry) + length;
							ixHeader.indexEntriesNumber += 1;
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.writePage(leafPage, pageData))
							{		
								free(pageData);
								free(pageData);
								return IX_MALLOC_FAILED;
							}
							free(pageData);
							free(keyVal);
							return SUCCESS;
						}           
						break;
					}
					default:
						break;
				}
			}
	}
	// split on non-leaf page
	// don't free pageData
	// free copy
	else
	{
		int offset = sizeof(IndexHeader)+INT_SIZE;
		NonLeafEntry dataEntry;
		dataEntry.child = leafPage;
		
		// copy entries to get the correct order
		bool inserted = false;
		for(int i = 0; i < numEntries; i++) 
		{
		        // get the next entry
		      	NonLeafEntry entry;
		        memcpy(&entry, (char*)pageData + offset, sizeof(NonLeafEntry));
		        offset += sizeof(NonLeafEntry);
		        // read the value in the entry. Compare with key and insert data entry
		        switch(attribute.type) 
		        {
		            case(TypeInt):
			    	{
						dataEntry.size = INT_SIZE;
						
			        	int nextInt;
			        	int keyInt;
			        	memcpy(&keyInt, key, INT_SIZE);
			        	memcpy(&nextInt, ((char*)pageData + offset), INT_SIZE);
						offset += INT_SIZE;
						         
			        	if(i == 0 && keyInt < nextInt)
			        	{
							// copy entry then rest of entries if key is less than first search key value
							memcpy( copy, &dataEntry, sizeof(NonLeafEntry));
							memcpy( (char*)copy + sizeof(NonLeafEntry), keyVal, INT_SIZE);
							memcpy( (char*)copy + sizeof(NonLeafEntry) + INT_SIZE, (char*)pageData + sizeof(IndexHeader)+INT_SIZE, ixHeader.freeSpaceOffset-sizeof(IndexHeader)-INT_SIZE);
							inserted = true;
						}
			        	else if( keyInt < nextInt)
						{
							// copy all entries up to current entry if key is less than current search key value
							// copy entry to be inserted
							// copy all remaining entries
							memcpy( copy, (char*)pageData + sizeof(IndexHeader)+INT_SIZE, offset-sizeof(IndexHeader) -INT_SIZE - sizeof(NonLeafEntry) - INT_SIZE;
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - INT_SIZE - sizeof(NonLeafEntry) - INT_SIZE, &dataEntry, sizeof(NonLeafEntry);
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - INT_SIZE - INT_SIZE, keyVal, INT_SIZE);
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - INT_SIZE, (char*)pageData + offset - sizeof(NonLeafEntry) - INT_SIZE, ixHeader.freeSpaceOffset-offset+sizeof(NonLeafEntry)+INT_SIZE);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && keyInt >= nextInt)
						{
							// copy all entries
							// then copy entry to be inserted
							// if key is greater than or equal to last search key value entry
							memcpy( copy, (char*)pageData + sizeof(IndexHeader)+INT_SIZE, ixHeader.freeSpaceOffset-sizeof(IndexHeader)-INT_SIZE);
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader)-INT_SIZE, &dataEntry, sizeof(NonLeafEntry));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader)-INT_SIZE+sizeof(NonLeafEntry), keyVal, INT_SIZE);
							inserted = true;
						}
						
						if(inserted)
						{
							// no need to split non-leaf node
							if( sizeof(NonLeafEntry) + INT_SIZE + ixHeader.freeSpaceOffset <= PAGE_SIZE)
							{
								memcpy( (char*)pageData + sizeof(IndexHeader) + INT_SIZE, copy, (numEntries+1)*(sizeof(NonLeafEntry)+INT_SIZE));
								ixHeader.indexEntriesNumber += 1;
								ixHeader.freeSpaceOffset += sizeof(NonLeafEntry)+INT_SIZE;
								setIndexHeader(pageData, ixHeader);
								void *p = malloc(PAGE_SIZE);
								if(ixfileHandle.readPage(leafPage,p))
								{
									free(p);
									free(pageData);
									free(copy);
									return FH_READ_FAILED;
								}
								IndexHeader childHeader = getIndexHeader(p);
								free(p);
								if(ixfileHandle.writePage(childHeader.parent, pageData))
								{
									free(pageData);
									free(copy);
									return FH_WRITE_FAILED;
								}
								free(pageData);
								free(copy);
								return SUCCESS;
							}
							else
							{
								// copy half of the entries to leafPage
								int half = floor((double)(numEntries+1)/2);
								memcpy( (char*)pageData + sizeof(IndexHeader)+INT_SIZE, (char*)copy, half*(sizeof(NonLeafEntry) + INT_SIZE));
								int oldRightSibling = ixHeader.right;
								ixHeader.right = ixfileHandle.getNumberOfPages();
								ixHeader.indexEntriesNumber = half;
								ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (half*(sizeof(LeafEntry)+INT_SIZE));
								setIndexHeader(pageData, ixHeader);
								if(ixfileHandle.writePage(leafPage, pageData)
								{
									free(pageData);
									free(copy);
									return FH_WRITE_FAILED;
								}
								// append numEntries+1- half to new page
								int remaining = numEntries+1-half;
								memcpy( (char*)pageData+sizeof(IndexHeader), (char*)copy+ (half*(sizeof(LeafEntry)+INT_SIZE)),remaining*(sizeof(LeafEntry)+INT_SIZE));
									// get middle entry to copy up
									void *middle = malloc(INT_SIZE);
									memcpy( &middle, (char*)copy + (half*(sizeof(LeafEntry)+INT_SIZE)) + sizeof(LeafEntry), INT_SIZE);
									 
								ixHeader.left = leafPage;
								ixHeader.right = oldRightSibling;
								ixHeader.indexEntriesNumber = remaining;
								ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (remaining*(sizeof(LeafEntry)+INT_SIZE));
								setIndexHeader(pageData, ixHeader);
								if(ixfileHandle.appendPage(pageData))
								{
									free(pageData);
									free(copy);
									free(middle);
									return IX_APPEND_FAILED;
								}
								free(copy);
							// update oldRightSibling's left ptr
							if( oldRightSibling == -1)
								continue;
							else
							{
								if(ixfileHandle.readPage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_READ_FAILED;
								}
								ixHeader = getIndexHeader(pageData);
								ixHeader.left = ixfileHeader.getNumberOfPages()-1;
								setIndexHeader(pageData);
								if(ixfileHandle.writePage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_WRITE_FAILED;
								}
							}
							// copy up middle entry to non-leaf node
							if(ixfileHandle.readPage(ixHeader.parent, pageData))
							{
								free(pageData);
								free(middle);
								return FH_READ_FAILED;	  
							}
							if(split(ixfileHandle, attribute, rid, leafPage, pageData, size, middle))
							{
								free(pageData);
								free(middle);
								return BAD_INPUT;
							}
							free(pageData);
							free(middle);
							return SUCCESS;
						}
		            	break; 
			    	}
		            case(TypeReal):
			    	{
						dataEntry.size = REAL_SIZE;
						
			        	float nextReal;
			        	float keyReal;
			        	memcpy(&keyReal, key, REAL_SIZE);
			        	memcpy(&nextReal, ((char*)pageData + offset), REAL_SIZE);
						offset += REAL_SIZE;
						         
			        	if(i == 0 && keyReal < nextReal)
			        	{
							// copy entry then rest of entries if key is less than first search key value
							memcpy( copy, &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + sizeof(LeafEntry), keyVal, REAL_SIZE);
							memcpy( (char*)copy + sizeof(LeafEntry) + REAL_SIZE, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							inserted = true;
						}
			        	else if( keyReal < nextReal)
						{
							// copy all entries up to current entry if key is less than current search key value
							// copy entry to be inserted
							// copy all remaining entries
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), offset-sizeof(IndexHeader) - sizeof(LeafEntry) - REAL_SIZE;
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - sizeof(LeafEntry) - REAL_SIZE, &dataEntry, sizeof(LeafEntry);
							memcpy( (char*)copy + offset - sizeof(IndexHeader) - REAL_SIZE, keyVal, REAL_SIZE);
							memcpy( (char*)copy + offset - sizeof(IndexHeader), (char*)pageData + offset - sizeof(LeafEntry) - REAL_SIZE, ixHeader.freeSpaceOffset-offset+sizeof(LeafEntry)+INT_SIZE);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && keyReal >= nextReal)
						{
							// copy all entries
							// then copy entry to be inserted
							// if key is greater than or equal to last search key value entry
							memcpy( copy, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)copy + ixHeader.freeSpaceOffset-sizeof(IndexHeader)+sizeof(LeafEntry), keyVal, REAL_SIZE);
							inserted = true;
						}
						
						if(inserted)
						{
							// copy half of the entries to leafPage
							int half = floor((double)(numEntries+1)/2);
							memcpy( (char*)pageData + sizeof(IndexHeader), (char*)copy, half*(sizeof(LeafEntry) + REAL_SIZE));
							int oldRightSibling = ixHeader.right;
							ixHeader.right = ixfileHandle.getNumberOfPages();
							ixHeader.indexEntriesNumber = half;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (half*(sizeof(LeafEntry)+REAL_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.writePage(leafPage, pageData)
							{
								free(pageData);
								free(copy);
								return FH_WRITE_FAILED;
							}
							// append numEntries+1- half to new page
							int remaining = numEntries+1-half;
							memcpy( (char*)pageData+sizeof(IndexHeader), (char*)copy+ (half*(sizeof(LeafEntry)+REAL_SIZE)),remaining*(sizeof(LeafEntry)+REAL_SIZE));
								// get middle entry to copy up
								void *middle = malloc(REAL_SIZE);
								memcpy( &middle, (char*)copy + (half*(sizeof(LeafEntry)+REAL_SIZE)) + sizeof(LeafEntry), REAL_SIZE);
								 
							ixHeader.left = leafPage;
							ixHeader.right = oldRightSibling;
							ixHeader.indexEntriesNumber = remaining;
							ixHeader.freeSpaceOffset = sizeof(IndexHeader) + (remaining*(sizeof(LeafEntry)+REAL_SIZE));
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.appendPage(pageData))
							{
								free(pageData);
								free(copy);
								free(middle);
								return IX_APPEND_FAILED;
							}
							free(copy);
							// update oldRightSibling's left ptr
							if( oldRightSibling == -1)
								continue;
							else
							{
								if(ixfileHandle.readPage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_READ_FAILED;
								}
								ixHeader = getIndexHeader(pageData);
								ixHeader.left = ixfileHeader.getNumberOfPages()-1;
								setIndexHeader(pageData);
								if(ixfileHandle.writePage(oldRightSibling,pageData))
								{
									free(pageData);
									free(middle);
									return FH_WRITE_FAILED;
								}
							}
							// copy up middle entry to non-leaf node
							if(ixfileHandle.readPage(ixHeader.parent, pageData))
							{
								free(pageData);
								free(middle);
								return FH_READ_FAILED;	  
							}
							if(split(ixfileHandle, attribute, rid, leafPage, pageData, size, middle))
							{
								free(pageData);
								free(middle);
								return BAD_INPUT;
							}
							free(pageData);
							free(middle);
							return SUCCESS;
						}
		            	break; 
		            }
			    	case(TypeVarChar):
			    	{ 
						 // get key value
						 int length;
						 memcpy(&length, (char*)key, INT_SIZE);
						 char* keyVarChar = (char*) malloc(length);
						 memcpy(keyVarChar, (char*)key + INT_SIZE, length);
								
						 dataEntry.size = length;
								
						 // get current search key value
						int nextLength = entry.size;
						char* nextVarChar = (char*)malloc(nextLength);  
						memcpy(nextVarChar, (char*) pageData + offset, nextLength);
						offset += nextLength;
					
					    if(i == 0 && strcmp(keyVarChar, nextVarChar) < 0)
					    {
							// memmov all entries if key is less than first search key value   
							memmove( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry) + length, (char*)pageData + sizeof(IndexHeader), ixHeader.freeSpaceOffset-sizeof(IndexHeader));
							memcpy( (char*)pageData + sizeof(IndexHeader), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + sizeof(IndexHeader) + sizeof(LeafEntry), keyVarChar, length);
							inserted = true;
						}
				        else if( strcmp(keyVarChar, nextVarChar) < 0)
						{
							// memmove all entries at current entry if key is less than current search key value
				        	memmove( (char*)pageData + offset - entry.size + length, (char*)pageData + offset - entry.size - sizeof(LeafEntry), ixHeader.freeSpaceOffset - offset + sizeof(LeafEntry) + entry.size);
							memcpy( (char*)pageData + offset - entry.size - sizeof(LeafEntry), &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + offset - entry.size, keyVarChar, length);
							inserted = true;
			        	}
			        	else if(i == numEntries - 1 && strcmp(keyVarChar, nextVarChar) >= 0)
						{
							// insert to end if key is greater than or equal to last search key value entry
							memcpy( (char*)pageData + offset, &dataEntry, sizeof(LeafEntry));
							memcpy( (char*)pageData + offset + sizeof(LeafEntry), keyVarChar, length);
							inserted = true;
						}
						// prevent memory leaks
						free(keyVarChar);
						free(nextVarChar);
						
						if(inserted)
						{
							ixHeader.freeSpaceOffset += sizeof(LeafEntry) + length;
							ixHeader.indexEntriesNumber += 1;
							setIndexHeader(pageData, ixHeader);
							if(ixfileHandle.writePage(leafPage, pageData))
							{		
								free(pageData);
								free(pageData);
								return IX_MALLOC_FAILED;
							}
							free(pageData);
							free(keyVal);
							return SUCCESS;
						}           
						break;
					}
					default:
						break;
				}
			}
	}
*/
	return SUCCESS;
}

