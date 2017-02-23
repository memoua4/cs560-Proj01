#include "../include/heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer", 
    "end of file encountered",
    "invalid update operation",
    "no space on page for record", 
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

// ********************************************************
// Constructor
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
    if (name == NULL) {
        fileName = "temp";
    } else {
        fileName = new char[strlen(name) + 1];
        strcpy(fileName, name);
    }

    Status status;

    if (name != NULL)
        status = MINIBASE_DB->get_file_entry(fileName, firstDirPageId);
    else
        status = OK;

    Page *newPage;

    if (status != OK) {
        status = MINIBASE_BM->newPage(firstDirPageId, newPage);
        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        status = MINIBASE_DB->add_file_entry(fileName, firstDirPageId);
        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        
    }

    // fill in the body
    returnStatus = OK;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // fill in the body
    deleted_file = 1;
    delete [] fileName;
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
   // fill in the body
   return OK;
}

// *****************************
// Insert a record into the file
/*
 * Function Name: insertRecord
 * @params: 
 *              recPtr = the record to be stored into data[] 
 *              recLen = the size of the record
 *              outRid = passed by reference to store the record being stored 
 *  Description: Initially, we want to pin the first page and page id into the buffer
 *  manager (request page in frame). It will then attempt to insert the record. If 
 *  it insert correctly, it will return OK as the status. There are multiple reasons why
 *  it didn't insert properly:
 *          1) The current page is full, it will then have to check for the next page
 *          2) There are no pages that have available space, therefore, it must create
 *          a new page and insert the record as the first record
 *  If it cannot insert, it should return DONE or FAIL?
 */
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    // fill in the body
    PageId curPageId = firstDirPageId; 
    PageId prevPageId = INVALID_PAGE;
    HFPage curPage;
    Status status;

    while (true) {
        // pin page
        MINIBASE_DB->pinPage(curPageId, (Page *)&curPage);
        status = curPage->insertRecord(recPtr, recLen, outRid);
        if ( status != OK ) {
            // get next page id
            prevPageId = curPageId;
            curPageId = curPage->getNextPage();
        }
        // unpin page
        MINIBASE_DB->unpinPage(curPageId);
    
        if ( status == OK ) 
            break;

        if ( curPageId == INVALID_PAGE ) {
            HFPage newPage;
            MINIBASE_DB->pinPage(prevPageId, (Page *)&curPage);
            MINIBASE_DB->newPage(curPageId, (Page *)&newPage);
            curPage->setNextPage(newPage);
            status = newPage->insertRecord(recPtr, recLen, outRid);
            MINIBASE_DB->unpinPage(prevPageId);
            break;
        }
    }
    return status;
} 


// ***********************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  // fill in the body
  return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
  // fill in the body
    PageId curPageId = firstDirPageId;
    PageId prevPageId;
    HFPage curPage;
    Status status;
    char *updateRec;
    int updateLen;

    while (curPageId != NULL) {
        MINIBASE_BM->pinPage(curPageId, (Page *&)curPage);
        //HFPage::getRecord(RID rid, char* recPtr, int& recLen)
        status = curPage->getRecord(rid, &updateRecord, &updateLen);
        if (status != OK) {
            prevPageId = curPageId;
            curPageId = curPage->getNextPage();
        }
        MINIBASE_BM->unpinPage(prevPageId);

        if ( status == OK ) 
            break;
    }
    if ( curPageId == INVALID_PAGE ) 
        return DONE; // file does not exists 

    if (updateLen != recLen)
        return FAIL; // file could not update

    memcpy(recPtr, updateRecord, recLen);
    return OK;

}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  // fill in the body 
  return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  // fill in the body 
    status = Scan::Scan(this, status);
    return NULL;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    // fill in the body
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    // fill in the body
    PageId newPageId;
    Page *newPage;
    Status status = MINIBASE_BM->newPage(newPageId, newPage); // create a new page

    if ( status != OK ) 
        return DONE;

    HFPage hfpage;
    hfpage->init(newPageId);

    dpinfop.availableSapce = hfpage->available_space();
    dpinfop.recct = 0;
    dpinfop.pageId = hfpage->page_no();
    return OK;
}

// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are 
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID& rid,
                    PageId &rpDirPageId, HFPage *&rpdirpage,
                    PageId &rpDataPageId,HFPage *&rpdatapage,
                    RID &rpDataPageRid)
{
    // fill in the body
    return OK;
}

// *********************************************************************
// Allocate directory space for a heap file page 

Status allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // fill in the body
    return OK;
}

// *******************************************
