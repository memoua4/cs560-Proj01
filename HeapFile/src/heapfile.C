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
HeapFile::HeapFile(const char *name, Status &returnStatus) {
    // Test to see if we're making a temporary directory or not
    // If we're making a temporary directory, use the file name "temp"
    if (name == NULL) {
        fileName = "temp";
    } else {
        fileName = new char[strlen(name) + 1];
        strcpy(fileName, name);
    }

    // Create a variable to hold the status of the various db & buffer manager calls
    Status status;

    // We can test to see if the DB has the file entry by attempting to retrieve it
    if (name != NULL)
        status = MINIBASE_DB->get_file_entry(fileName, firstDirPageId);
    else
        status = OK;

    Page *firstPage;

    // Since we did not get OK, we need to create the first page
    if (status != OK) {
        // Create a page
        status = MINIBASE_BM->newPage(firstDirPageId, firstPage);
        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        // Have the DB add the page to the file
        status = MINIBASE_DB->add_file_entry(fileName, firstDirPageId);
        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        // Now that we have the header page allocated, we need to initialize it since the constructor does not do that
        // We can cast a page to an HFPage since it "is a" page
        ((HFPage *) firstPage)->init(firstDirPageId);

        // Now that we have the page, initialized, we don't need it anymore so unpin it from the buffer manager
        status = MINIBASE_BM->unpinPage(firstDirPageId, true);
        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }
    }

    // Initialize the file_deleted flag to false as stated in the variable
    file_deleted = false;
    returnStatus = OK;
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
   // fill in the body
    file_deleted = 1;
    delete [] fileName;
}


// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt() {
    Page *currentPage;
    // Grab the first page ID
    PageId currentPageID = firstDirPageId;
    // Accumulator
    int totalRecordCount = 0;

    // Loop while we don't have an invalid page
    while (currentPageID != INVALID_PAGE) {
        // Pin the page and load it into currentPage
        Status status = MINIBASE_BM->pinPage(currentPageID, currentPage);
        // Ensure that the pin worked
        if (status != OK)
            return -1;

        // Cast the page to an HFPage
        HFPage *castedCurrentPage = (HFPage *) currentPage;

        //
        // Start Count the records on the page
        //

        int recordCount = 0;
        RID currentRID;
        // Grab the first record
        Status hasNext = castedCurrentPage->firstRecord(currentRID);
        // While we have another record...
        while (hasNext == OK) {
            // Accumulate the counter
            recordCount = recordCount + 1;
            RID nextRID;
            // Grab the next RID
            hasNext = castedCurrentPage->nextRecord(currentRID, nextRID);
            // Set the current RID
            currentRID = nextRID;
        }

        // Accumulate with the total
        totalRecordCount = totalRecordCount + recordCount;

        //
        // End Count the records on the page
        //

        // Grab the next page id, and free the current page
        PageId next = castedCurrentPage->getNextPage();
        status = MINIBASE_BM->unpinPage(currentPageID);
        if (status != OK)
            return -1;
        currentPageID = next;
    }

    return totalRecordCount;
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
        MINIBASE_BM->pinPage(curPageId, (Page *&)curPage);
        status = curPage.insertRecord(recPtr, recLen, outRid);
        if ( status != OK ) {
            // get next page id
            prevPageId = curPageId;
            curPageId = curPage.getNextPage();
        }
        // unpin page
        MINIBASE_BM->unpinPage(curPageId);
    
        if ( status == OK ) 
            break;

        if ( curPageId == INVALID_PAGE ) {
            HFPage newPage;
            MINIBASE_BM->pinPage(prevPageId, (Page *&)curPage);
            MINIBASE_BM->newPage(curPageId, (Page *&)newPage);
            curPage.setNextPage(newPage.page_no());
            status = newPage.insertRecord(recPtr, recLen, outRid);
            MINIBASE_BM->unpinPage(prevPageId);
            break;
        }
    }
    return status;
}


// ***********************
// delete record from file
Status HeapFile::deleteRecord(const RID &rid) {
    PageId pageID = rid.pageNo;
    Page *page;
    Status status;

    status = MINIBASE_BM->pinPage(pageID, page);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    HFPage *hfPage = (HFPage *) page;

    // If we get "DONE", no records were deleted
    status = hfPage->deleteRecord(rid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    RID temp;
    // If the page is empty, delete the data page if it is not the first page
    if (pageID != firstDirPageId && (hfPage->firstRecord(temp) == DONE)) {
        PageId previousPageID = hfPage->getPrevPage();
        PageId nextPageID = hfPage->getPrevPage();

        //
        // Free & Unpin the current page
        //

        status = MINIBASE_BM->unpinPage(pageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        status = MINIBASE_BM->freePage(pageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        //
        // Pin the previous page and update it
        //

        if (previousPageID != INVALID_PAGE) {
            status = MINIBASE_BM->pinPage(previousPageID, page);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

            hfPage = (HFPage *) page;
            hfPage->setNextPage(nextPageID);

            status = MINIBASE_BM->unpinPage(previousPageID, true);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }

        //
        // Pin the next page and update it
        //

        if (nextPageID != INVALID_PAGE) {
            status = MINIBASE_BM->pinPage(nextPageID, page);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

            hfPage = (HFPage *) page;
            hfPage->setPrevPage(previousPageID);

            status = MINIBASE_BM->unpinPage(nextPageID, true);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }
    }

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
        status = curPage.getRecord(rid, updateRec, updateLen);
        if (status != OK) {
            prevPageId = curPageId;
            curPageId = curPage.getNextPage();
        }
        MINIBASE_BM->unpinPage(prevPageId);

        if ( status == OK ) 
            break;
    }
    if ( curPageId == INVALID_PAGE ) 
        return DONE; // file does not exists 

    if (updateLen != recLen)
        return FAIL; // file could not update

    memcpy(recPtr, updateRec, recLen);
    return OK;

}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord(const RID &rid, char *recPtr, int &recLen) {
    Status status;
    Page *page;
    PageId pageId;

    pageId = rid.pageNo;
    status = MINIBASE_BM->pinPage(pageId, page);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    HFPage *hfPage = (HFPage *) page;

    status = hfPage->getRecord(rid, recPtr, recLen);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
  // fill in the body 
    Scan scan = Scan::Scan(this, status);
    return &scan;
}

// ****************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile() {
    // If we already deleted the file, can't delete it again
    if (file_deleted)
        return MINIBASE_FIRST_ERROR(HEAPFILE, ALREADY_DELETED);

    file_deleted = true;

    Status status;
    PageId currentPageID = firstDirPageId;
    Page *currentPage;

    // Go through the linked list of pages and delete each one
    while (currentPageID != INVALID_PAGE) {
        // Pin the page to get the next page ID
        status = MINIBASE_BM->pinPage(currentPageID, currentPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        PageId next = ((HFPage *) currentPage)->getNextPage();

        // Unpin the page
        status = MINIBASE_BM->unpinPage(currentPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Free the page
        status = MINIBASE_BM->freePage(currentPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Advance the next pointer
        currentPageID = next;
    }

    // Delete the file from the DB
    status = MINIBASE_DB->delete_file_entry(fileName);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

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
    MINIBASE_BM->newPage(newPageId, newPage); // create a new page

    HFPage hfpage;
    hfpage.init(newPageId);

    dpinfop->availspace = hfpage.available_space();
    dpinfop->recct = 0;
    dpinfop->pageId = hfpage.page_no();
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
