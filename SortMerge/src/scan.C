/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "../include/heapfile.h"
#include "../include/scan.h"
#include "../include/hfpage.h"
#include "../include/buf.h"
#include "../include/db.h"

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
/**
  * Function: Scan:Scan
  * Parameter: HeapFile *hf : the filename of the Heap File
  *                    Status (Passed By Referenced) This is used to indicate whether or not a Scan object was successfully created
  *
  * Description:  The constructor creates a Scan object. Scan::init(HeapFile *hf) page is called to initialized all the private 
  * variables in the header file. Refer to Scan::init(hf) to indicate the exact description of the constructor. 
  */
Scan::Scan(HeapFile *hf, Status &status) {
    status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
/** 
 * Function: Scan::-Scan()
 * 
 * Description: The deconstructor calls reset() which will reset all the global variables and unpin all the pages.
 */
Scan::~Scan() {
    // put your code here
    reset();
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
/**
 * Function: Scan::getNext(RID &rid, char *recPtr, int &recLen) 
 * Parameter: RID rid ( passed by reference ) is used to store the record id of the next record on the page. 
 *                    char * recPtr ( passed by reference ) stores the content of the record that it retrieves 
 *                    int &recLen ( passed by reference ) stores the length of the record
 *
 *  @return: status 
 *            status is used to indicate whether or not the next record was retrieved successfully 
 *            it returns OK if it retrives the next record
 *            it returns DONE or other Messages if the next record was not able to be retrieved
 *
 * Description: This method is used to retrieved the next record on the data page. It does a look ahead to check if the 
 * data page still contains a record that have not been examined. If the nxtUserStatus (which is used to indicate that
 * the next record exists) is not OK, it calls the nextDataPage(). This will attempt to retrieve the first record on the next
 * data page. If successful, it will return the first record on that data page. Otherwise, if nxtUserStatus is OK, it return
 * the following record.
 */
Status Scan::getNext(RID &rid, char *recPtr, int &recLen) {
    Status status;

    // Check if we have a valid datapage to grab from
    if (nxtUserStatus != OK) {
        status = nextDataPage();
        if (status == DONE) {
            return DONE;
        } else if (status != OK) {
            return MINIBASE_CHAIN_ERROR(SCAN, status);
        }
    }

    // Grab all the other data we need to return
    // This will fill in recPtr and recLen
    status = dataPage->getRecord(userRid, recPtr, recLen);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    // Fill in rid
    rid = userRid;

    // Move userRid to the next location, and put that result into the nxtUserStatus
    // variable indicating if we have another record
    nxtUserStatus = dataPage->nextRecord(userRid, userRid);

    return status;
}

// *******************************************
// Do all the constructor work.
/**
 * Scan::init(HeapFile *hf) 
 * Parameter: HeapFile *hf : the name of the heapfile 
 * 
 * return status 
 *            indicates if the Scan object has been created 
 *            
 * Description: The method first set the heapfile and set scanIsDone to false. When creating a scan object, scanIsDone
 * must be set to false. The heapfile has not been accessed yet. 
 */
Status Scan::init(HeapFile *hf) {
    // put your code here
    _hf = hf; // set the heapfile name
    scanIsDone = false; // 0 indicates that the scan is not finished yet
    return firstDataPage(); // get the first page
}

// *******************************************
// Reset everything and unpin all pages.
/** 
 * Function: Scan::reset()
 *
 * return status
 *              indcates whether or not the method has unpinned all the pages in the Buffer Manager
 *
 * Description: This function resets all the pages pinned in the pool. It resets all the HFPage (individual page) to NULL.
 * This indicates that the scan has been deleted.
 */
Status Scan::reset() {
    Status status;
    if (dataPageId != INVALID_PAGE) {
        status = MINIBASE_BM->unpinPage(dataPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(SCAN, status);

        dataPageId = INVALID_PAGE;
    }
    if (dataPage != NULL) {
        dataPage = NULL;
    }
    if (dirPageId != INVALID_PAGE) {
        status = MINIBASE_BM->unpinPage(dirPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(SCAN, status);
        dirPageId = _hf->firstDirPageId;
    }
    if (dirPage != NULL) {
        // Unpin the dirPage
        dirPage = NULL;
    }
    scanIsDone = false;
    nxtUserStatus = OK;
    return OK;
}

// *******************************************
// Copy data about first page in the file.
/**
 * Function: firstDataPage()
 * 
 * @return status 
 *            retrieves the first data page that exists in the directory 
 *
 * Description: This function attempts to retrieve the first data page in the first directory page. If it fails to grab the first
 * data page, it will return an error message. If it does grab the first data page, the buffer manager pins the first directory 
 * page into the pool. Then it calls nextDataPage() to retrieved the next record or checks if the next record exists.
 */
Status Scan::firstDataPage() {
    // put your code here
    Status status;
    dirPageId = _hf->firstDirPageId;
    scanIsDone = 0;
    nxtUserStatus = OK;

    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    status = dirPage->firstRecord(dataPageRid);
    if (status != OK && status == DONE) {
        reset();
        return DONE; // no record exists in the data page
    }

    DataPageInfo *dataPageInfo = new DataPageInfo();
    int len;
    status = dirPage->getRecord(dataPageRid, (char *) dataPageInfo, len);
    if ( status != OK ) 
      return MINIBASE_CHAIN_ERROR(SCAN, status);

    dataPageId = dataPageInfo->pageId;
    delete dataPageInfo;

    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    Status gotFirst = dataPage->firstRecord(userRid);
    if (gotFirst == DONE) return DONE;
    else if (gotFirst == OK) return OK;
    else return MINIBASE_CHAIN_ERROR(SCAN, gotFirst);
}

// *******************************************
// Retrieve the next data page.
/**
 * Function: nextDataPage() 
 * 
 * return status 
 *          status determines if a next record exsits (if it doesn't exists) this means it reached the end of the heapfile
*                       no directory has any more data pages.
 * 
 * Description: It attemps to retrieved the next record in the data page. If the next record does not exists, it will check the
 * following directory page. If it reaches the end of the file, it will return DONE, otherwise it returns OK. If it returned 
 * other error messages, this may be because of the Buffer Manager failing to pin and unpin pages. 
 */
Status Scan::nextDataPage() {
    Status status;

    // Retrieve the next DataPageInfo
    RID nextDataPageInfoRID;
    // Retrieve the next dataPage from the directory
    status = dirPage->nextRecord(dataPageRid, nextDataPageInfoRID);
    // Done means it's time to move onto the next directory page
    if (status == DONE) {
        Status hasNextDirPage = nextDirPage();
        if (hasNextDirPage == OK) {
            // Retrieve the next dataPage from the directory
            status = dirPage->firstRecord(nextDataPageInfoRID);
        }
            // if no next directory pages exists, we're done with the scan
        else if (hasNextDirPage == DONE) {
            reset();
            return DONE;
        } else return MINIBASE_CHAIN_ERROR(SCAN, status);
    }
        // Move on ONLY if status == ok
    else if (status != OK) return MINIBASE_CHAIN_ERROR(SCAN, status);

    // We got the next dataPageRid to read from
    dataPageRid = nextDataPageInfoRID;

    // Grab the DataPageInfo from the directory page
    DataPageInfo *info = new DataPageInfo();
    int length;
    status = dirPage->getRecord(dataPageRid, (char *) info, length);
    if ( status != OK ) {
      status = MINIBASE_BM->unpinPage(dataPageId);
      if (status != OK)
          return MINIBASE_CHAIN_ERROR(SCAN, status);
    }

    // Unpin the current data page
    status = MINIBASE_BM->unpinPage(dataPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);
    dataPageId = INVALID_PAGE;

    // Set the new dataPageId
    dataPageId = info->pageId;
    delete info;

    // Set the new dataPage
    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    dataPage->firstRecord(userRid);
    nxtUserStatus = OK;

    return OK;
}

// *******************************************
// Retrieve the next directory page.
/** 
 * Function: nextDirPage()
 * 
 * Description: The function returns a status message. If it reaches the end of the Heap File, it returns DONE, otherwise OK
 * because it successfully got the next directory page. Other error messages are indicated if fails in the Buffer Manager. 
 */
Status Scan::nextDirPage() {
    PageId oldDirPage = dirPageId;
    dirPageId = dirPage->getNextPage();
    Status status;
    status = MINIBASE_BM->unpinPage(oldDirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);
    if (dirPageId == INVALID_PAGE)
        return DONE; // reached the end of the file
    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);
    return OK;
}

Status Scan::position(RID rid) {
    reset();
    firstDataPage();

    RID tempRid;
    char *tempRec = new char[1000];
    int tempLen;
    while (userRid != rid) {
        Status status = getNext(tempRid, tempRec, tempLen);
        if (status != OK)
            return status;
    }
    delete tempRec;
    return OK;
}