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
Scan::Scan (HeapFile *hf, Status& status)
{
  // put your code here
  status = OK;
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
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
  // put your code here
  return OK;
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
  // put your code here
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

  status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
  if ( status != OK ) 
    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

  status = dirPage->firstRecord(dataPageRid);
  if ( status != OK && status == DONE ) 
    return DONE; // no record exists in the data page 

  status = MINIBASE_BM->unpinPage(dirPageId);
  if ( status != OK ) 
    return MINIBASE_CHAIN_ERROR(HEAPFILE, status);

  userRid = dataPageRid;

  status = nextDataPage(); // check if next data page exists 
  if ( status != OK ) 
    return DONE; // only one file 

  return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
  // put your code here
  return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
  // put your code here
  dirPageId = dirPage->getNextPage();
  if ( dirPageId == INVALID_PAGE ) {
    return DONE; // reached the end of the file
  }
  return OK;
}
 