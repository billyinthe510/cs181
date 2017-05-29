
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
	// Failed to read page 0q
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
	return -1;

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



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{

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
			newEntry.size = varcharlen;
			memcpy( (char*)pageData + freeSpace, &newEntry, sizeof(NonLeafEntry));
			freeSpace += sizeof(NonLeafEntry);

			memcpy( (char*)pageData + freeSpace, (char*)key + INT_SIZE, varcharlen);
			freeSpace += varcharlen;
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

    NonLeafEntry *prevEntry;	
    int prevSize = 0;
    for(int i = 0; i < numEntries; i++) 
    {
        // get the next entry
        NonLeafEntry *entry;
        memcpy(entry, (char*)pageData + offset, sizeof(NonLeafEntry));
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
			memcpy(prevEntry, ((char*)pageData + offset - (2*INT_SIZE) - (2*sizeof(NonLeafEntry))), sizeof(NonLeafEntry));
			return prevEntry->child;	
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry
	            if(i == numEntries - 1 && keyInt >= nextInt)
			return entry->child;

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
	                memcpy(prevEntry, (char*)pageData + offset - (2*REAL_SIZE) - (2*sizeof(NonLeafEntry)), sizeof(NonLeafEntry));
	                return prevEntry->child;
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry
	            if(i == numEntries - 1 && keyReal >= nextReal)
	              	return entry->child;
					
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
		    int nextLength = entry->size;
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
	                memcpy(prevEntry, (char*)pageData + offset - nextLength - (2*sizeof(NonLeafEntry)) - prevSize, sizeof(NonLeafEntry));
	                return prevEntry->child;
	            }
		    // return last page pointer if key is greater than or equal to last search key value entry   
	            if(i == numEntries - 1 && strcmp(keyVarChar, nextVarChar) >= 0)
	    		return entry->child;
					
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
    void *keyVal;
    switch(attribute.type)
    {
	case TypeInt:
	{
	    size = INT_SIZE;
	    memcpy( (uint32_t*) keyVal, key, INT_SIZE);
	    break;
	}
	case TypeReal:
	{
	    size = REAL_SIZE;
	    memcpy( (float*)keyVal, key, REAL_SIZE);
	    break;
	}
	case TypeVarChar:
	{
	    memcpy( &size, key, VARCHAR_LENGTH_SIZE);
	    memcpy( (char*)keyVal, (char*)key + VARCHAR_LENGTH_SIZE, size);
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
		    // a single entry should be less than PAGE_SIZE
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
		free(pageData);
		return FH_WRITE_FAILED;
	}
	free(pageData);
	return SUCCESS;
    }

    for(int i=0; i < ixHeader.indexEntriesNumber; i++)
    {

    	switch(attribute.type)
    	{
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
	
		    break;
		}
		default:
		    break;
   	 }
    }
    return 0;
}
