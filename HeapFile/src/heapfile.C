#include "../include/heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {"bad record id", "bad record pointer", "end of file encountered",
                                  "invalid update operation", "no space on page for record",
                                  "page is empty - no records", "last record on page", "invalid slot number",
                                  "file has already been deleted",};

static error_string_table hfTable(HEAPFILE, hfErrMsgs);

// ********************************************************
// Constructor
HeapFile::HeapFile(const char *name, Status &returnStatus)
{
    // Test to see if we're making a temporary directory or not
    // If we're making a temporary directory, use the file name "temp"
    if (name == NULL)
    {
        fileName = "temp";
    } else
    {
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
    if (status != OK)
    {
        // Create a page
        status = MINIBASE_BM->newPage(firstDirPageId, firstPage);
        if (status != OK)
        {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        // Have the DB add the page to the file
        status = MINIBASE_DB->add_file_entry(fileName, firstDirPageId);
        if (status != OK)
        {
            returnStatus = MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return;
        }

        // Now that we have the header page allocated, we need to initialize it since the constructor does not do that
        // We can cast a page to an HFPage since it "is a" page
        ((HFPage *) firstPage)->init(firstDirPageId);
        // cout << "Space Constructor = " << ((HFPage *) firstPage)->available_space() << endl;
        // Now that we have the page, initialized, we don't need it anymore so unpin it from the buffer manager
        status = MINIBASE_BM->unpinPage(firstDirPageId, true);
        if (status != OK)
        {
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
    if ( file_deleted != 1 ) { // the file has not been deleted
        Status status = deleteFile();

        if ( status != OK ) {
            return;
        }
    }
}


// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    HFPage *currentPage;
    // Grab the first page ID
    PageId currentPageID = firstDirPageId;
    // Accumulator
    int totalRecordCount = 0;

    // Go through each directory page
    while (currentPageID != INVALID_PAGE)
    {
        Status status;

        // Pin the directory page
        MINIBASE_BM->pinPage(currentPageID, (Page *&) currentPage);

        // Load the first record on the page
        DataPageInfo *currentInfo;
        RID currentRID;
        Status retStatus = currentPage->firstRecord(currentRID);
        if (retStatus == OK)
        {
            // Loop while we have another record to analyze
            do
            {
                // Grab the record as a DataPageInfo
                int length;
                currentPage->getRecord(currentRID, (char *) currentInfo, length);
                // Accumulate to the total
                totalRecordCount = totalRecordCount + currentInfo->recct;
            } while (currentPage->nextRecord(currentRID, currentRID) == OK);
        }

        // Unpin the directory page and advance to the next page
        PageId old = currentPageID;
        currentPageID = currentPage->getNextPage();
        MINIBASE_BM->unpinPage(old);
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
Status HeapFile::insertRecord(char *recPtr, int recLen, RID &outRid)
{
    HFPage *dirPage; 
    PageId dirPageId = firstDirPageId;
    PageId prevDirPageId;
    RID dirRecId;
    PageId dataPageId = INVALID_PAGE;
    HFPage *dataPage;
    PageId prevDataPageId;
    DataPageInfo *dpinfop = new DataPageInfo;
    Status status;

    while (true) {
        MINIBASE_BM->pinPage(dirPageId, (Page *&)dirPage);

        Status firstStatus = dirPage->firstRecord(dirRecId);
    
        while ( true ) {
            if ( status != OK ) {
                break;
            }
            MINIBASE_BM->pinPage(dirRecId.pageNo, (Page *&)dataPage);
            if ( dataPage->available_space() >= recLen ) {
                dataPageId = dirRecId.pageNo;
                break;
            }
            prevDataPageId = dirRecId.pageNo;
            MINIBASE_BM->unpinPage(dirRecId.pageNo);
            status = dirPage->nextRecord(dirRecId, dirRecId);
        }
        if ( dataPageId != INVALID_PAGE ) {
            MINIBASE_BM->pinPage(dataPageId, (Page *&)dataPage);
            status = dataPage->insertRecord(recPtr, recLen, outRid);
            MINIBASE_BM->unpinPage(dataPageId);
            return OK;
        } else {
            if (dirPage->available_space() >= recLen) {
                newDataPage(dpinfop);
                dataPageId = dpinfop->pageId;
                if (firstStatus == DONE) {
                    MINIBASE_BM->pinPage(prevDataPageId, (Page *&)dataPage);
                    dataPage->setNextPage(dataPageId);
                    MINIBASE_BM->unpinPage(prevDataPageId);
                }

                MINIBASE_BM->pinPage(dataPageId, (Page *&)dataPage);
                dataPage->setPrevPage(prevDataPageId);
                cout << "Here I am...." << dataPage->available_space() << endl;
                status = dataPage->insertRecord(recPtr, recLen, outRid);
                if ( status != OK ) {
                    cout << "Cannot insert for some reason...." << endl;
                    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                }
                MINIBASE_BM->unpinPage(dataPageId);
                break;
            } else {
                prevDirPageId = dirPageId;
                dirPageId = dirPage->getNextPage();
                if ( dirPageId == INVALID_PAGE ) {
                    MINIBASE_BM->unpinPage(prevDirPageId);
                    allocateDirSpace(dpinfop, prevDirPageId, dirRecId);
                    dataPageId = dpinfop->pageId;
                    // insert here
                    MINIBASE_BM->pinPage(prevDirPageId, (Page *&)dirPage);
                    MINIBASE_BM->pinPage(dirRecId.pageNo, (Page *&)dataPage);
                    status = dataPage->insertRecord(recPtr, recLen, outRid);
                    if ( status != OK ) {
                        cout << "cannot insert for some othe reason...." << endl;
                        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                    }
                    MINIBASE_BM->unpinPage(dirRecId.pageNo);
                    MINIBASE_BM->unpinPage(prevDirPageId);
                    break;
                } else {
                    MINIBASE_BM->unpinPage(prevDirPageId);
                    MINIBASE_BM->pinPage(dirPageId, (Page *&)dirPage);
                }
            }
        }
    }


    return OK;
}


// ***********************
// delete record from file
Status HeapFile::deleteRecord(const RID &rid)
{
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;
    Status status;

    status = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage, dirRID);
    if (status == OK)
    {
        dataPage->deleteRecord(rid);
        bool dataEmpty = dataPage->empty();
        if (dataEmpty)
        {
            dirPage->deleteRecord(dirRID);
        }

        MINIBASE_BM->unpinPage(dataPageID, true);
        MINIBASE_BM->unpinPage(dirPageID, true);
        if (dataEmpty)
            MINIBASE_BM->freePage(dataPageID);
    }

    return OK;
}


// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord(const RID &rid, char *recPtr, int recLen)
{
    PageId rpDirPageId, rpDataPageId;
    HFPage *rpdirpage, *rpdatapage;
    RID rpDataPageRid;

    Status status = findDataPage(rid, rpDirPageId, rpdirpage, rpDataPageId, rpdatapage, rpDataPageRid);

    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    char *foundRec = NULL;
    int foundLen;

    status = MINIBASE_BM->pinPage(rpDataPageId, (Page *&) rpdatapage);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    status = rpdatapage->getRecord(rid, foundRec, foundLen);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    status =  MINIBASE_BM->unpinPage(rpDataPageId);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    if (recLen != foundLen) 
        return FAIL;

    memcpy(recPtr, foundRec, recLen);
    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord(const RID &rid, char *recPtr, int &recLen)
{
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;
    Status status;

    status = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage, dirRID);
    if (status == OK)
    {
        dataPage->getRecord(rid, recPtr, recLen);
    }

    return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status &status)
{
  // fill in the body 
    Scan * scan =  new Scan(this, status);
    return scan;
}

// ***************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile()
{
    // If we already deleted the file, can't delete it again
    if (file_deleted)
        return MINIBASE_FIRST_ERROR(HEAPFILE, ALREADY_DELETED);

    file_deleted = true;

    Status status;
    PageId currentDirPageID = firstDirPageId;
    HFPage *currentDirPage;

    // Go through the linked list of pages and delete each one
    while (currentDirPageID != INVALID_PAGE)
    {
        // Pin the page to get the next page ID
        MINIBASE_BM->pinPage(currentDirPageID, (Page *&) currentDirPage);

        // Grab the first record on that directory page
        RID currentDirRecord;
        status = currentDirPage->firstRecord(currentDirRecord);
        if (status != DONE)
        {
            // Loop through each record in the directory page
            do
            {
                // Load the record from the directory page
                DataPageInfo *pageInfo;
                int count;
                currentDirPage->getRecord(currentDirRecord, (char *) pageInfo, count);
                MINIBASE_BM->freePage(pageInfo->pageId);
            } while (currentDirPage->nextRecord(currentDirRecord, currentDirRecord) != OK);
        }

        PageId next = currentDirPage->getNextPage();
        MINIBASE_BM->unpinPage(currentDirPageID);
        MINIBASE_BM->freePage(currentDirPageID);
        currentDirPageID = next;
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
    Status status = MINIBASE_BM->newPage(newPageId, newPage); // create a new page

    if ( status != OK ) {
        return MINIBASE_FIRST_ERROR(HEAPFILE, status);
    }

    HFPage hfpage;
    hfpage.init(newPageId);

    dpinfop->availspace = hfpage.available_space();
    dpinfop->recct = 0;
    dpinfop->pageId = newPageId;
   
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
// return a data page (rpDataPageId, rpdatapage) containing a given record (rid)
// as well as a directory page (rpDirPageId, rpdirpage) containing the data page and RID of the data page (rpDataPageRid)
Status HeapFile::findDataPage(const RID &rid, PageId &rpDirPageId, HFPage *&rpdirpage, PageId &rpDataPageId,
                              HFPage *&rpdatapage, RID &rpDataPageRid)
{
    Status status;
    HFPage *currentDirPage;
    PageId currentDirPageID = firstDirPageId;

    // Loop through each directory page in the linked list
    while (currentDirPageID != INVALID_PAGE)
    {
        MINIBASE_BM->pinPage(currentDirPageID, (Page *&) currentDirPage);

        // Grab the first record on that directory page
        RID currentDirRecord;
        status = currentDirPage->firstRecord(currentDirRecord);
        if (status == DONE)
        {
            rpdirpage = NULL;
            rpdatapage = NULL;
            return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
        }

        // Loop through each record in the directory page
        do
        {
            // Load the record from the directory page
            DataPageInfo *pageInfo;
            int count;
            currentDirPage->getRecord(currentDirRecord, (char *) pageInfo, count);

            // Check to see if the current RID is the correct one
            if (pageInfo->pageId == rid.pageNo)
            {
                // If it is, fill in all parameters and return
                rpDataPageId = pageInfo->pageId;
                MINIBASE_BM->pinPage(pageInfo->pageId, (Page *&) *rpdatapage);
                rpDirPageId = currentDirPageID;
                rpdirpage = currentDirPage;
                rpDataPageRid = currentDirRecord;
                return OK;
            }

        } while (currentDirPage->nextRecord(currentDirRecord, currentDirRecord) != OK);

        PageId old = currentDirPageID;
        // Move on to the next page
        currentDirPageID = currentDirPage->getNextPage();
        // Unpin the previous page
        MINIBASE_BM->unpinPage(old);
    }

    rpdirpage = NULL;
    rpdatapage = NULL;
    return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
}

// *********************************************************************
// Allocate directory space for a heap file page 
Status HeapFile::allocateDirSpace(struct DataPageInfo * dpinfop,
                            PageId &allocDirPageId,
                            RID &allocDataPageRid)
{
    // // fill in the body
    Page * newDirPage;
    PageId newDirPageId;

    MINIBASE_BM->newPage(newDirPageId, newDirPage);
    HFPage * dirPage = (HFPage *) newDirPage;
    dirPage->init(newDirPageId);
    dirPage->setPrevPage(allocDirPageId);

    dpinfop->availspace = dirPage->available_space();
    dpinfop->recct = 0;
    dpinfop->pageId = newDirPageId;

    DataPageInfo *info = new DataPageInfo;
    newDataPage(info);
    allocDataPageRid.pageNo = info->pageId;
    allocDataPageRid.slotNo = info->recct;

    HFPage curDirPage;
    MINIBASE_BM->pinPage(allocDirPageId, (Page *&)curDirPage);
    curDirPage.setNextPage(newDirPageId);
    MINIBASE_BM->unpinPage(allocDirPageId);

    allocDirPageId = newDirPageId; // set new page 

    return OK;

}

// *******************************************
