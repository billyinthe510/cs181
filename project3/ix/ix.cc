
#include "ix.h"

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
	void *firstPageData = calloc(PAGE_SIZE, 1);
	if(firstPageData == NULL)
		return IX_MALLOC_FAILED;
	newRecordBasedPage(firstPageData);

	// Adds the first index based page
	FileHandle fileHandle;
	if (pfm->openFile(fileName.c_str(), fileHandle) )
		return IX_OPEN_FAILED;
	if(fileHandle.appendPage(firstPageData))
		return IX_APPEND_FAILED;
	pfm->closeFile(fileHadle);

	free(firstPageData);

	return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
	if( !isIndexFile(fileName))
	    return IX_DESTROY_FAILED;
	return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	// Validate index file
	if( !isIndexFile(fileName))
		return IX_OPEN_FAILED;
	
	// If this handle already has an open file, error
	if( ixfileHandle.getfd() != NULL)
		return PFM_HANDLE_IN_USE;
	// If the file doesn't exist, error
	if( !fileExists(fileName.c_str()))
		return PFM_FILE_DN_EXIST;
	// Open the file for reading/writing in binary mode
	FILE *pFile;
	pFile = fopen(fileName.c_str(),"rb+");
	// If failed to open, error
	if( pFile == NULL)
		return IX_OPEN_FAILED;
	
	ixfileHandle.setfd(pFile);

	return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if( !isIndexFile(fileName))
		return IX_CLOSE_FAILED;
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
    return -1;
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
	fd = NULL;
}

IXFileHandle::~IXFileHandle()
{
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
	fd = fd;
}
FILE *IXFileHandle::getfd()
{
	return fd;
}
// HELPER FUNCTIONS
bool IXFileManager::isIndexFile(const string &fileName)
{
	return  strcmp( fileName.substr(fileName.size()-3) , ".ix") == 0
}
