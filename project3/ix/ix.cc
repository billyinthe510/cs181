
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
        //cout << pfm->createFile(ixFile);
	if(pfm->createFile(ixFile))
		return IX_CREATE_FAILED;
	
	// Setting up the first page
	void *firstPageData = calloc(PAGE_SIZE, 1);
	if(firstPageData == NULL)
		return IX_MALLOC_FAILED;
	newIndexBasedPage(firstPageData);

	// Add p0 pointer to page 1
	unsigned p0 = 1;
	memcpy( firstPageData + sizeof(IndexHeader), &p0, sizeof(unsigned) );

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
	ixHeader.freeSpaceOffset -= sizeof(unsigned);
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
    uint16_t freeSpace = ixHeader.freeSpaceOffset;
    // If indexEntriesNmber is 0 then tree must be empty
    if( ixHeader.indexEntriesNumber == 0)
    {
	// Set index file's attribute type
	ixfileHandle.setAttr(attribute.type);

	// add key value to root node 
	switch(attribute.type)
	{
		case TypeInt:
		{
			memcpy(pageData + freeSpace, key, INT_SIZE);
			freeSpace += INT_SIZE;
			break;
		}
		case TypeReal:
		{
			memcpy(pageData + freeSpace, key, REAL_SIZE);
			freeSpace += REAL_SIZE;
			break;
		}
		case TypeVarChar:
		{
			int varcharlen;
			memcpy(&varcharlen, key, INT_SIZE);
			memcpy(pageData + freeSpace, key, INT_SIZE + varcharlen);
			freeSpace += INT_SIZE + varcharlen;
			break;
		}
	}
	// add p1 pointer to root node (2 or next pageNum)
	unsigned p1 = ixfileHandle.getNumberOfPages();
	memcpy(pageData + freeSpace, &p1, sizeof(unsigned));
	freeSpace += sizeof(unsigned);	
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
	free(pageData);

	// set up a leaf page for first index entry
	void *leafPageData = malloc(PAGE_SIZE);
	newIndexBasedPage(leafPageData);
	ixHeader = getIndexHeader(leafPageData);
	freeSpace = ixHeader.freeSpaceOffset;
	ixHeader.isLeafPage = true;
	// for leaf pages, remove the default p0 pointer from freeSpaceOffset
	freeSpace -= sizeof(unsigned);
	// add data entry key value to leaf node
	switch(attribute.type)
	{
		case TypeInt:
		{
			memcpy(leafPageData + freeSpace, key, INT_SIZE);
			freeSpace += INT_SIZE;
			break;
		}
		case TypeReal:
		{
			memcpy(leafPageData + freeSpace, key, REAL_SIZE);
			freeSpace += REAL_SIZE;
			break;
		}
		case TypeVarChar:
		{
			int varlen;
			memcpy(&varlen, key, INT_SIZE);
			memcpy(leafPageData + freeSpace, key, INT_SIZE + varlen);
			freeSpace += INT_SIZE + varlen;
			break;
		}
	}
	// add data entry RID to leaf node
	memcpy(leafPageData + freeSpace, &rid, sizeof(RID));
	freeSpace += sizeof(RID);
	// update leaf page's header
	ixHeader.freeSpaceOffset = freeSpace;
	ixHeader.parent = 0;
	ixHeader.left = 1;
	ixHeader.indexEntriesNumber = 1;
	setIndexEntriesNumber(leafPageData, ixHeader);
	// write leaf page to disk
	if( ixfileHandle.appendPage(leafPageData))
	{
		free(leafPageData);
		return IX_APPEND_FAILED;
	}
	free(leafPageData);

	// update p0's neighbor
	void *p0 = malloc(PAGE_SIZE);
	if( p0 == NULL)
		return  IX_MALLOC_FAILED;

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
	ixHeader.right = ixfileHandle.getNumberOfPages() - 1;
	setIndexHeader(p0, ixHeader);
	// write p0's page 1 to disk
	if( ixfileHandle.writePage(1, p0))
	{
		free(p0);
		return FH_WRITE_FAILED;
	}
	free(p0);

	return SUCCESS;
    } 	
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



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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
    ixHeader.freeSpaceOffset = sizeof(IndexHeader) + sizeof(unsigned);
    ixHeader.indexEntriesNumber = 0;
    ixHeader.isLeafPage = false;
    ixHeader.parent = -1;
    ixHeader.left = -1;
    ixHeader.right = -1;
    setIndexHeader(data, ixHeader);
}


