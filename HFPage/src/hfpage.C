#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
    curPage = pageNo;
    nextPage = INVALID_PAGE;
    prevPage = INVALID_PAGE;
    slotCnt = 0; 
    usedPtr = MAX_SPACE - DPFIXED;
    freeSpace = MAX_SPACE - DPFIXED;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    return prevPage;
    // return 0;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    prevPage = pageNo;
    // fill in the body
}

// **********************************************************
PageId HFPage::getNextPage()
{
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    if (available_space() >= recLen) {
        int i;
        for (i = 0; slot[i] != EMPTY_SLOT && i <= slotCnt; i++ ) { }

        rid->pageNo = curPage;
        rid->slotNo = i;
        slot[i]->offset = usedPtr - recLen;
        slot[i]->length = recLen;
        memcpy(slot[i]->offset,recPtr, recLen);
        usedPtr = usedPtr - recLen;
        freeSpace = freeSpace - recLen - sizeof(slot_t);
        
        if ( i == slotCnt ) {
            slotCnt++;
        }
    } else {
        return DONE;
    }
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    if (rid->pageNo == curPage) {
        if ( rid->slotNo < slotCnt ) {
            memmove(usedPtr+slot[rid->slotNo]->length, 
                                usedPtr, 
                                slot[rid->slotNo]->offset - usedPtr);

            for (int i = 0; i < rid->slotNo; i++ ) {
                if (slot[i]->length != EMPTY_SLOT ) {
                    slot[i]->offset += slot[rid->slotNo]->length;
                }
            }

            freeSpace = freeSpace + slot[rid->slotNo]->offset;
            usedPtr = usedPtr + slot[rid->slotNo]->offset;
            
            slot[rid->slotNo] -> length = EMPTY_SLOT;
            slot[rid->slotNo]->offset = 0;  
        
        }
    } else {
        return FAIL;
    }
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    int i;
    for (i = 0; slot[i]->length != EMPTY_SLOT && i <= slotCnt; i++ ) {}
    if ( i == slotCnt ) 
        return DONE;
    firstRid->pageNo = curPage;
    firstRid->slotNo = i;
   return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    if (curRid->pageNo != curPage ) 
        return FAIL;
    if (slot[curRid->slotNo]->length == EMPTY_SLOT ) 
        return FAIL;
    if (curRid->slotNo >= slotCnt) 
        return FAIL; 

    int i;
    for ( i = curRid->slotNo + 1; i < slotCnt; i++ ) {
        if (slot[i]->length != EMPTY_SLOT) {
            nextRid->pageNo = curPage;
            nextRid->slotNo = i;
            return OK;
        }
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // fill in the body
    if (rid->pageNo != curPage) 
        return FAIL;
    if (slot[rid->slotNo]->length == EMPTY_SLOT) 
        return FAIL;
    if (rid->slotNo >= slotCnt) 
        return FAIL;

    memcpy(recPtr, slot[rid->slotNo]->offset, slot[rid->slotNo]->length);
    recLen = slot[rid->slotNo]->length;
    
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    // fill in the body
    if (rid->pageNo != curPage) 
        return FAIL;
    if (slot[rid->slotNo]->length == EMPTY_SLOT) 
        return FAIL;
    if (rid->slotNo >= slotCnt) 
        return FAIL;

    recPtr = slot[rid->slotNo]->offset;
    recLen = slot[rid->slotNo]->length;
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    return freeSpace - sizeof(slot_t);
    // return 0;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    int i;
    for ( i = 0; i < slotCnt; i++ ) {
        if ( slot[i]->length != EMPTY_SLOT ) 
            return false;
    }
    return true;
}



