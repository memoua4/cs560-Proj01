#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "../include/hfpage.h"
#include "../include/buf.h"
#include "../include/db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
    // fill in the body
    // initialize the curPage, prevPage, and nextPage
    curPage = pageNo;
    prevPage = INVALID_PAGE;
    nextPage = INVALID_PAGE;
    // initialize slotCnt starts at index 2 since index 1 is already occupied
    slotCnt = 0;
    // Initialize slot array
    slot[0].length = INVALID_SLOT;
    // initialize usedPtr, points to the end of data arrayx
    usedPtr = MAX_SPACE - DPFIXED;
    // initialize freeSpace, it is equivalent to the fixed number of space in the data array
    freeSpace = MAX_SPACE - DPFIXED;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace << ", slotCnt=" << slotCnt << endl;

    for (i = 0; i <= slotCnt; i++)
    {
        cout << "slot[" << i << "].offset=" << slot[i].offset << ", slot[" << i << "].length=" << slot[i].length
             << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    // fill in the body
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    // fill in the body
    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // fill in the body
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    // fill in the body
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid)
{
    // fill in the body
    if (recLen > freeSpace)
        return DONE;

    int i;
    for (i = 0; i <= slotCnt; i++)
    {
        if (slot[i].length == EMPTY_SLOT)
            break;
    }

    rid.pageNo = curPage;
    rid.slotNo = i;
    usedPtr -= recLen;
    slot[i].offset = usedPtr;
    slot[i].length = recLen;
    memcpy(&data[slot[i].offset], recPtr, recLen);
    freeSpace = freeSpace - recLen - sizeof(slot_t);
    if (i > slotCnt)
        slotCnt++;
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID &rid)
{
    // fill in the body
    if (rid.pageNo != curPage)
        return FAIL;
    int no = rid.slotNo;
    if (no < 0 || no > slotCnt)
        return FAIL;
    int offset = slot[no].offset;
    int deletedRecLen = slot[no].length;

    // reset all the flag and free the struct
    slot[no].offset = -1;
    slot[no].length = EMPTY_SLOT;

    // shift all the records to the right
    char * destination = data + usedPtr + deletedRecLen;
    char * source = data + usedPtr;
    size_t numBytes = offset - usedPtr;
    memmove(destination, source, numBytes);


    // adjust where the usedPtr now points
    usedPtr = usedPtr + deletedRecLen;
    // since the record is deleted, add the available memory back to the freeSpace
    freeSpace = freeSpace + deletedRecLen;
    // adjust all the offset for slot[rid->slot] who is not empty
    // and its offset must be less than the original offset of the deleted record
    for (int i = 0; i <= slotCnt; i++)
    {
        if (slot[i].length != EMPTY_SLOT && slot[i].offset < offset)
        {
            slot[i].offset += deletedRecLen;
        }
    }
    // Make sure to grab the extra space back after deletion from the end of the slot array
    for (int i = slotCnt; i >= 0; i--)
    {
        if (slot[i].length != EMPTY_SLOT)
            break;
        slotCnt = slotCnt - 1;
        freeSpace =  freeSpace + sizeof(slot_t);
    }
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID &firstRid)
{
    // fill in the body
    for (int i = 0; i <= slotCnt; i++)
    {
        if (slot[i].length != EMPTY_SLOT)
        {
            firstRid.pageNo = curPage;
            firstRid.slotNo = i;
            return OK;
        }
    }
    return DONE;   // this indicates that no record exists
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord(RID curRid, RID &nextRid)
{
    // fill in the body
    int curNo = curRid.slotNo;
    if (curRid.pageNo != curPage)
        return FAIL;
    if (curNo < 0)
        return FAIL;
    if (curNo > slotCnt)
        return DONE;
    if (slot[curNo].length == EMPTY_SLOT)
        return FAIL;

    for (int i = curNo + 1; i <= slotCnt; i++)
    {
        if (slot[i].length != EMPTY_SLOT)
        {
            nextRid.pageNo = curPage;
            nextRid.slotNo = i;
            return OK;
        }
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char *recPtr, int &recLen)
{
    // fill in the body
    if (rid.pageNo != curPage)
        return FAIL;
    int no = rid.slotNo;
    if (slot[no].length == EMPTY_SLOT)
        return FAIL;

    int offset = slot[no].offset;
    recLen = slot[no].length;
    memcpy(recPtr, &data[offset], recLen);
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char *&recPtr, int &recLen)
{
    // fill in the body
    if (rid.pageNo != curPage)
        return FAIL;
    int no = rid.slotNo;
    if (slot[no].length == EMPTY_SLOT)
        return FAIL;

    int offset = slot[no].offset;
    recLen = slot[no].length;
    recPtr = &data[offset];
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    return freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    for (int i = 0; i < slotCnt; i++)
    {
        if (slot[i].length != EMPTY_SLOT)
            return false;
    }
    return true;
}