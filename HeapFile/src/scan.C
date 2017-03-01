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
Scan::Scan(HeapFile *hf, Status &status)
{
    status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
    // put your code here
    reset();
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID &rid, char *recPtr, int &recLen)
{
    Status status;


    // Check if we have a valid datapage to grab from
    if (nxtUserStatus != OK)
    {
        status = nextDataPage();
        if (status == DONE) {
            return DONE;
        }
        else if (status != OK) {
            return MINIBASE_CHAIN_ERROR(SCAN, status);
        }
    }

    // Grab all the other data we need to return
    // This will fill in recPtr and recLen
    status = dataPage->getRecord(userRid, recPtr, recLen);
    if (status != OK) {
        return MINIBASE_CHAIN_ERROR(SCAN, status);
    }

    // Fill in rid
    rid = userRid;

    // Move userRid to the next location, and put that result into the nxtUserStatus
    // variable indicating if we have another record
    nxtUserStatus = dataPage->nextRecord(userRid, userRid);

    return status;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
    // put your code here
    _hf = hf; // set the heapfile name
    scanIsDone = 0; // 0 indicates that the scan is not finished yet
    return firstDataPage(); // get the first page
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
    if (dataPage != NULL)
    {
        dataPage = NULL;
    }
    if (dataPageId != INVALID_PAGE) {
        MINIBASE_BM->unpinPage(dataPageId);
        dataPageId = 0;
    }
    if (dirPage != NULL)
    {
        // Unpin the dirPage
        dirPage = NULL;
    }
    if (dirPageId != INVALID_PAGE) {
        MINIBASE_BM->unpinPage(dirPageId);
        dirPageId = _hf->firstDirPageId;
    }
    scanIsDone = false;
    nxtUserStatus = OK;
    return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
    // put your code here
    Status status;
    dirPageId = _hf->firstDirPageId;
    scanIsDone = 0;
    dataPage = NULL;
    nxtUserStatus = OK;

    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

    status = dirPage->firstRecord(dataPageRid);
    if (status != OK && status == DONE)
        return DONE; // no record exists in the data page

    DataPageInfo *dataPageInfo = new DataPageInfo();
    int len;
    dirPage->getRecord(dataPageRid, (char *) dataPageInfo, len);

    dataPageId = dataPageInfo->pageId;

    status = nextDataPage(); // check if next data page exists

    if (status != OK)
        return DONE; // only one file

    return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage()
{
    Status status;

    // dataPage might be null if this is the first call to nextDataPage
    if (dataPage == NULL)
    {
        // Make sure that this current page ID is not invalid
        if (dataPageId == INVALID_PAGE) return DONE;

        // Pin the page
        MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
        // Grab the first record and place RID into the private data field
        Status gotFirst = dataPage->firstRecord(userRid);
        // If we got a first record, just return it
        if (gotFirst == OK) return OK;
        else if (gotFirst == DONE) return DONE;
        else return MINIBASE_CHAIN_ERROR(SCAN, gotFirst);
    }

    // Unpin the current data page
    MINIBASE_BM->unpinPage(dataPageId);
    dataPageId = INVALID_PAGE;

    // Retrieve the next DataPageInfo
    RID nextDataPageInfoRID;
    // Retrieve the next dataPage from the directory
    status = dirPage->nextRecord(dataPageRid, nextDataPageInfoRID);
    // Done means it's time to move onto the next directory page
    if (status == DONE)
    {
        Status hasNextDirPage = nextDirPage();
        if (hasNextDirPage == OK)
        {
            // Retrieve the next dataPage from the directory
            status = dirPage->firstRecord(nextDataPageInfoRID);
        }
            // if no next directory pages exists, we're done with the scan
        else if (hasNextDirPage == DONE)
        {
            scanIsDone = true;
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
    dirPage->getRecord(dataPageRid, (char *) info, length);

    // Set the new dataPageId
    dataPageId = info->pageId;
    // Set the new dataPage
    MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);

    dataPage->firstRecord(userRid);
    nxtUserStatus = OK;

    return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage()
{
    PageId oldDirPage = dirPageId;
    dirPageId = dirPage->getNextPage();
    MINIBASE_BM->unpinPage(oldDirPage);
    if (dirPageId == INVALID_PAGE)
    {
        return DONE; // reached the end of the file
    }
    MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    return OK;
}
 