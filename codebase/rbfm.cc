#include "rbfm.h"
#include "pfm.h"

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

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    PagedFileManager *pfManager = PagedFileManager::instance();
    return pfManager->createFile(fileName);
    


}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    PagedFileManager *pfManager = PagedFileManager::instance();
    return pfManager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    PagedFileManager *pfManager = PagedFileManager::instance();
    return pfManager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    PagedFileManager *pfManager = PagedFileManager::instance();
    return pfManager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    return -1;
}
