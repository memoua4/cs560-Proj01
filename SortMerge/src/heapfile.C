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
HeapFile::HeapFile(const char *name, Status &returnStatus) {
    // Test to see if we're making a temporary directory or not
    // If we're making a temporary directory, use the file name "XtempX"
    if (name == NULL) {
        fileName = new char[10];
        strcpy(fileName, "XtempX");
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
        // cout << "Space Constructor = " << ((HFPage *) firstPage)->available_space() << endl;
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
HeapFile::~HeapFile() {
    // Just delete the file name, and the file if we have a temp file
    if (strcmp(fileName, "XtempX") == 0 && file_deleted == false)
        deleteFile();
    delete[] fileName;
}


// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt() {
    HFPage *currentPage;
    // Grab the first page ID
    PageId currentPageID = firstDirPageId;
    // Accumulator
    int totalRecordCount = 0;
    // Status
    Status status;

    // Go through each directory page
    while (currentPageID != INVALID_PAGE) {
        // Pin the directory page
        status = MINIBASE_BM->pinPage(currentPageID, (Page *&) currentPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Load the first record on the page
        DataPageInfo *currentInfo = new DataPageInfo();
        RID currentRID;
        Status retStatus = currentPage->firstRecord(currentRID);
        if (retStatus == OK) {
            // Loop while we have another record to analyze
            do {
                // Grab the record as a DataPageInfo
                int length;
                retStatus = currentPage->getRecord(currentRID, (char *) currentInfo, length);
                // Accumulate to the total
                if (retStatus == OK)
                    totalRecordCount = totalRecordCount + currentInfo->recct;
            } while (currentPage->nextRecord(currentRID, currentRID) == OK);
        }

        // Unpin the directory page and advance to the next page
        PageId old = currentPageID;
        currentPageID = currentPage->getNextPage();
        status = MINIBASE_BM->unpinPage(old);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
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
Status HeapFile::insertRecord(char *recPtr, int recLen, RID &outRid) {

    // We can only accept records smaller than 1000 bytes
    if (recLen > 1000)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, FAIL);

    HFPage *dirPage;
    PageId dirPageId = firstDirPageId;
    PageId nextDirPageId;
    RID dirRecId;
    PageId dataPageId;
    HFPage *dataPage;
    DataPageInfo *dataPageInfo = new DataPageInfo();
    Status status;

    // This loop tests for the pages we already have
    while (dirPageId != INVALID_PAGE) {
        status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Grab the first record
        Status gotFirst = dirPage->firstRecord(dirRecId);

        if (gotFirst == OK) {
            // Loop through each of the directory DataPageInfo structs
            do {
                int tempLen;
                // FIRST PERFORM A REGULAR GET RECORD
                status = dirPage->getRecord(dirRecId, (char *&) dataPageInfo, tempLen);
                if (status == OK)
                    // If one has enough space, insert into it
                    if (dataPageInfo->availspace >= recLen) {
                        // if we got it, use RETURN RECORD to get access to the actual memeory
                        status = dirPage->returnRecord(dirRecId, (char *&) dataPageInfo, tempLen);
                        if (status == OK)
                        {
                            // Grab the data page id
                            dataPageId = dataPageInfo->pageId;
                            // Pin the data page
                            status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            // Insert the record
                            dataPage->insertRecord(recPtr, recLen, outRid);
                            // Update the info struct
                            dataPageInfo->availspace = dataPage->available_space();
                            dataPageInfo->recct++;
                            // Unpin the data and directory pages, then return ok
                            status = MINIBASE_BM->unpinPage(dataPageId, true);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            status = MINIBASE_BM->unpinPage(dirPageId, true);
                            if (status != OK)
                                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                            return OK;
                        }
                    }
            // Loop until we don't have a next record
            } while (dirPage->nextRecord(dirRecId, dirRecId) == OK);
        }

        // Advance to the next directory page
        nextDirPageId = dirPage->getNextPage();
        status = MINIBASE_BM->unpinPage(dirPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        dirPageId = nextDirPageId;
    }

    // Need a new data page, so allocate one
    status = newDataPage(dataPageInfo);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    // Allocate space for the datapageInfo struct
    status = allocateDirSpace(dataPageInfo, dirPageId, dirRecId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);


    // Grab the page ID
    dataPageId = dataPageInfo->pageId;
    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    int tempLen;
    // Insert the record (must succeed since we just created it!)
    dataPage->insertRecord(recPtr, recLen, outRid);
    // Return the info struct to increment recct and avail space
    dirPage->returnRecord(dirRecId, (char *&) dataPageInfo, tempLen);
    dataPageInfo->recct++;
    dataPageInfo->availspace = dataPage->available_space();
    status = MINIBASE_BM->unpinPage(dataPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(dirPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);


    return OK;
}


// ***********************
// delete record from file
Status HeapFile::deleteRecord(const RID &rid) {
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;
    Status status;

    // Find the record to delete
    status = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage, dirRID);
    if (status == OK) {
        // Delete the record from the actual data page
        status = dataPage->deleteRecord(rid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        // Check if we can delete the data page too
        bool dataEmpty = dataPage->empty();
        // If it's empty, delete the dataPageInfo struct too
        if (dataEmpty) {
            status = dirPage->deleteRecord(dirRID);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }

        // Unpin the pages, and free the dataPageID if we deleted it
        status = MINIBASE_BM->unpinPage(dataPageID, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->unpinPage(dirPageID, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        if (dataEmpty) {
            status = MINIBASE_BM->freePage(dataPageID);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        }
    } else
        return status;

    return OK;
}


// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord(const RID &rid, char *recPtr, int recLen) {
    PageId rpDirPageId, rpDataPageId;
    HFPage *rpdirpage, *rpdatapage;
    RID rpDataPageRid;

    // Find the data page that the record we're updating is in
    Status status = findDataPage(rid, rpDirPageId, rpdirpage, rpDataPageId, rpdatapage, rpDataPageRid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    // Variables to hold the status if we've found the record or not
    char *foundRec = NULL;
    int foundLen;

    // Return the record pointer
    status = rpdatapage->returnRecord(rid, foundRec, foundLen);
    if (status != OK) {
        status = MINIBASE_BM->unpinPage(rpDataPageId, false);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->unpinPage(rpDirPageId, false);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    // Make sure the lengths match so that we can update properly
    if (recLen != foundLen) {
        status = MINIBASE_BM->unpinPage(rpDataPageId, false);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->unpinPage(rpDirPageId, false);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    // Copy the memory overriding the old location
    memcpy(foundRec, recPtr, recLen);

    // Unpin the pages, and return OK
    status = MINIBASE_BM->unpinPage(rpDataPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(rpDirPageId, false);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord(const RID &rid, char *recPtr, int &recLen) {
    PageId dataPageID;
    HFPage *dataPage;
    PageId dirPageID;
    HFPage *dirPage;
    RID dirRID;
    Status status;

    // Get the record to be updated
    status = findDataPage(rid, dirPageID, dirPage, dataPageID, dataPage, dirRID);
    if (status == OK) {
        // Data page must have the record since we just found it in the above call
        dataPage->getRecord(rid, recPtr, recLen);
    } else {
        status = MINIBASE_BM->unpinPage(dataPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->unpinPage(dirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    status = MINIBASE_BM->unpinPage(dataPageID);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(dirPageID);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status &status) {
    // Create a new scan object, and return it
    Scan *scan = new Scan(this, status);
    return scan;
}

// ***************************************************
// Wipes out the heapfile from the database permanently. 
Status HeapFile::deleteFile() {
    // If we already deleted the file, can't delete it again
    if (file_deleted)
        return MINIBASE_FIRST_ERROR(HEAPFILE, ALREADY_DELETED);

    // We now deleted the file
    file_deleted = true;

    Status status;
    PageId currentDirPageID = firstDirPageId;
    HFPage *currentDirPage;

    // Go through the linked list of pages and delete each one
    while (currentDirPageID != INVALID_PAGE) {
        // Pin the page to get the next page ID
        status = MINIBASE_BM->pinPage(currentDirPageID, (Page *&) currentDirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Grab the first record on that directory page
        RID currentDirRecord;
        status = currentDirPage->firstRecord(currentDirRecord);
        if (status != DONE) {
            // Loop through each record in the directory page
            do {
                // Load the record from the directory page
                DataPageInfo *pageInfo = new DataPageInfo();
                int count;
                // Grab the page info, and free the page
                currentDirPage->getRecord(currentDirRecord, (char *) pageInfo, count);
                status = MINIBASE_BM->freePage(pageInfo->pageId);
                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
                delete pageInfo;
            //Loop while we have another record
            } while (currentDirPage->nextRecord(currentDirRecord, currentDirRecord) == OK);
        }

        // Move to the next page
        PageId next = currentDirPage->getNextPage();
        status = MINIBASE_BM->unpinPage(currentDirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
        status = MINIBASE_BM->freePage(currentDirPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
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
Status HeapFile::newDataPage(DataPageInfo *dpinfop) {
    PageId newPageId;
    HFPage *newPage;
    Status status;

    // Allocate a new page
    status = MINIBASE_BM->newPage(newPageId, (Page *&) newPage); // create a new page
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    // Init the page
    newPage->init(newPageId);

    // Create the DataPageInfo struct
    dpinfop->availspace = newPage->available_space();
    dpinfop->recct = 0;
    dpinfop->pageId = newPageId;

    // Unpin the page id
    status = MINIBASE_BM->unpinPage(newPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

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
                              HFPage *&rpdatapage, RID &rpDataPageRid) {
    Status status;
    HFPage *currentDirPage;
    PageId currentDirPageID = firstDirPageId;


    // Loop through each directory page in the linked list
    while (currentDirPageID != INVALID_PAGE) {
        status = MINIBASE_BM->pinPage(currentDirPageID, (Page *&) currentDirPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Grab the first record on that directory page
        RID currentDirRecord;
        status = currentDirPage->firstRecord(currentDirRecord);
        if (status == DONE) {
            rpdirpage = NULL;
            rpdatapage = NULL;
            return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
        }

        DataPageInfo *pageInfo = new DataPageInfo();

        // Loop through each record in the directory page
        do {
            // Load the record from the directory page
            int count;
            currentDirPage->getRecord(currentDirRecord, (char *) pageInfo, count);

            // Check to see if the current RID is the correct one
            if (pageInfo->pageId == rid.pageNo) {
                // If it is, fill in all parameters and return
                rpDataPageId = pageInfo->pageId;
                MINIBASE_BM->pinPage(rpDataPageId, (Page *&) rpdatapage);
                rpDirPageId = currentDirPageID;
                rpdirpage = currentDirPage;
                rpDataPageRid = currentDirRecord;
                delete pageInfo;
                return OK;
            }
        // Advance to the next record
        } while (currentDirPage->nextRecord(currentDirRecord, currentDirRecord) == OK);

        delete pageInfo;

        PageId old = currentDirPageID;
        // Move on to the next page
        currentDirPageID = currentDirPage->getNextPage();
        currentDirPage = NULL;
        // Unpin the previous page
        status = MINIBASE_BM->unpinPage(old);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    rpdirpage = NULL;
    rpdatapage = NULL;
    return MINIBASE_FIRST_ERROR(HEAPFILE, RECNOTFOUND);
}

// *********************************************************************
// Allocate directory space for a heap file page 
Status HeapFile::allocateDirSpace(struct DataPageInfo *dataPageInfoPtr, PageId &allocDirPageId, RID &allocDataPageRid) {
    PageId currentPageID = firstDirPageId;
    PageId lastPageID = INVALID_PAGE;
    HFPage *currentPage;
    HFPage *lastPage;
    Status status;

    // Loop through each directory page
    while (currentPageID != INVALID_PAGE) {
        status = MINIBASE_BM->pinPage(currentPageID, (Page *&) currentPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

        // Try and insert the dataPageInfo
        RID insertRID;
        Status insertSuccessful = currentPage->insertRecord((char *) dataPageInfoPtr, sizeof(DataPageInfo), insertRID);
        // Insert into the page
        if (insertSuccessful == OK) {
            allocDirPageId = currentPageID;
            allocDataPageRid = insertRID;
            status = MINIBASE_BM->unpinPage(currentPageID, true);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
            return OK;
        }

        // Move to the next page
        lastPageID = currentPageID;
        currentPageID = currentPage->getNextPage();
        status = MINIBASE_BM->unpinPage(lastPageID);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    }

    PageId newPageId;

    // If we get here, we need a new dir page
    status = MINIBASE_BM->newPage(newPageId, (Page *&) currentPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    currentPage->init(newPageId);
    currentPage->setPrevPage(lastPageID);

    // Update the previous page to point to the new page
    status = MINIBASE_BM->pinPage(lastPageID, (Page *&) lastPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    lastPage->setNextPage(newPageId);
    status = MINIBASE_BM->unpinPage(lastPageID, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);
    status = MINIBASE_BM->unpinPage(newPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    // Insert into the new page MUST be successful
    RID insertRID;
    currentPage->insertRecord((char *) dataPageInfoPtr, sizeof(DataPageInfo), insertRID);
    allocDirPageId = newPageId;
    allocDataPageRid = insertRID;
    return OK;
}

// *******************************************
