#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan

#define IX_CREATE_FAILED 10
#define IX_OPEN_FAILED 11
#define IX_APPEND_FAILED 12
#define IX_DESTROY_FAILED 13
#define IX_CLOSE_FAILED 14
#define IX_MALLOC_FAILED 15
#define IX_NULL_FILE 16
#define ERROR_FINDING_LEAF_PAGE 0
#define POINTER_ERROR 17
#define BAD_INPUT 18

typedef struct IndexHeader
{
    uint16_t freeSpaceOffset;
    uint16_t indexEntriesNumber;
    bool isLeafPage;
    int parent;
    int left;
    int right;
} IndexHeader;

typedef struct NonLeafEntry{
	int size;
	uint32_t child;
}NonLeafEntry;

typedef struct LeafEntry{
	int size;
	RID rid;
} LeafEntry;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);
	// Helper functions
	bool isIndexFile(const string &fileName);
	uint32_t getNextChild(void* pageData, const Attribute &attribute, const void* key);
	uint32_t findLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *pageData);
		// helpers for insert will free pageData for us
	RC insertIntoLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, uint32_t &leafPage, void *pageData);
	RC insertIntoEmptyTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *pageData);
	RC split(IXFileHandle &ixfileHandle, const Attribute &attribute, const RID &rid, uint32_t leafPage, void *pageData, int size, void *keyVal);
	
	bool getIsLeafPage(IXFileHandle &ixfileHandle, uint32_t pageNum);
	void printNonLeaf(IXFileHandle &ixfileHandle, uint32_t pageNum, int numSpaces, const Attribute &attribute);
	void printLeaf(IXFileHandle &ixfileHandle, uint32_t pageNum, int numSpaces, const Attribute &attribute);
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
	 PagedFileManager *pfm;
        bool fileExists(const string &fileName);
        void newIndexBasedPage(void *data);
        void setIndexHeader(void *data, IndexHeader ixHeader);
	IndexHeader getIndexHeader(void *data);
};


class IX_ScanIterator {
    public:

	// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void*data);
    unsigned getNumberOfPages();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
	void setfd(FILE *fd);
	FILE *getfd();
	void setAttr(AttrType type);
	AttrType getAttr();
private:
	FILE *_fd;
	AttrType attr_type;
};

#endif
