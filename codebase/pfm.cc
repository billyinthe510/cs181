#include "pfm.h"
#include <cstdio>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();
    
    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    struct stat stFileInfo;
    
    if(stat(fileName.c_str(), &stFileInfo) == 0) return -1;
    ofstream myFile;
    myFile.open(fileName.c_str());
    myFile.close();
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    struct stat stFileInfo;
    
    if(stat(fileName.c_str(), &stFileInfo) != 0) return -1;
    remove(fileName.c_str());
    return 0;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    struct stat stFileInfo;
    if(stat(fileName.c_str(), &stFileInfo) != 0) return -1;
    if (fileHandle.getFP() != NULL)
    {
        return -1;
    }
    FILE *FP = fopen(fileName.c_str(), "r + w"); // read/update and write/update on
    fileHandle.setFP(FP);
    return 0;
    /*struct stat stFileInfo;
     
     if(stat(fileName.c_str(), &stFileInfo) != 0) return -1;
     if(fileHandle.isUsed == true) return -1;
     //fstream myFile;
     //myFile.open(fileName);
     fileHandle.fileName.open(fileName);
     fileHandle.isUsed = true;
     return 0;*/
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    fclose(fileHandle.getFP());
    fileHandle.setFP(NULL);
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pageCounter = 0;
    FP = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    FILE *FP = getFP();
    if (pageNum > getNumberOfPages() || pageNum < 0)
    {
        return -1;
    }
    
    if (FP != NULL)
    {
        fseek(FP, pageNum * PAGE_SIZE, SEEK_SET); // beginning of page
        fread(data, 1, PAGE_SIZE, FP);
        readPageCounter++;
        return 0;
    }
    else
        return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    FILE *FP = getFP();
    if (pageNum > getNumberOfPages() || pageNum < 0)
    {
        return -1;
    }
    
    if (FP != NULL)
    {
        fseek(FP, pageNum * PAGE_SIZE, SEEK_SET); // beginning of page
        fwrite(data, 1, PAGE_SIZE, FP);
        writePageCounter++;
        return 0;
    }
    else
        return -1;
}


RC FileHandle::appendPage(const void *data)
{
    FILE *FP = getFP();
    if (FP != NULL)
    {
        fseek(FP, 0, SEEK_END);
        fwrite(data, 1, PAGE_SIZE, FP);
        pageCounter++;
        appendPageCounter++;
        return 0;
    }
    else
        return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    FILE *FP = getFP();
    if (FP == NULL)
    {
        return -1;
    }
    return pageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

void FileHandle::setFP(FILE *file)
{
    FP = file;
}

FILE* FileHandle::getFP()
{
    return FP;
}

