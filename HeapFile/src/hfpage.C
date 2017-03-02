#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "../include/hfpage.h"
#include "../include/buf.h"
#include "../include/db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo) {
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
void HFPage::dumpPage() {
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace << ", slotCnt=" << slotCnt << endl;

    for (i = 0; i <= slotCnt; i++) {
        cout << "slot[" << i << "].offset=" << slot[i].offset << ", slot[" << i << "].length=" << slot[i].length
             << endl;
    }
}

// **********************************************************
// Just return the previous page
PageId HFPage::getPrevPage() {
    return prevPage;
}

// **********************************************************
// Set the previous page
void HFPage::setPrevPage(PageId pageNo) {
    prevPage = pageNo;
}

// **********************************************************
// Return the next page
PageId HFPage::getNextPage() {
    return nextPage;
}

// **********************************************************
// Set the next page
void HFPage::setNextPage(PageId pageNo) {
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid) {
    // Ensure we have enough space to insert the record
    if ((recLen + sizeof (slot_t)) > freeSpace)
        return DONE;

    int i;
    // Scan for an empty slot, and save that value into i
    for (i = 0; i <= slotCnt; i++) {
        if (slot[i].length == EMPTY_SLOT)
            break;
    }

    // Set the page number and slot number to the current page and i
    rid.pageNo = curPage;
    rid.slotNo = i;
    // Move usedPtr backwards the size of the record
    usedPtr -= recLen;
    // Set the slot offset
    slot[i].offset = usedPtr;
    slot[i].length = recLen;
    // Copy the memory found at recPtr into the right slot
    memcpy(&data[slot[i].offset], recPtr, recLen);
    // Reduce the free space available
    freeSpace = freeSpace - recLen - sizeof(slot_t);
    // Increment the slot count
    if (i > slotCnt)
        slotCnt++;
    return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID &rid) {
    // Make sure it's the right page
    if (rid.pageNo != curPage)
        return FAIL;
    // Grab the slot number
    int no = rid.slotNo;
    // Ensure that the slot is valid
    if (no < 0 || no > slotCnt)
        return FAIL;
    // Grab the offset and length
    int offset = slot[no].offset;
    int deletedRecLen = slot[no].length;

    // reset all the flag and free the struct
    slot[no].offset = -1;
    slot[no].length = EMPTY_SLOT;

    // shift all the records to the right
    // The destination is the deletedRecLen to the "right"/"down"
    char *destination = data + usedPtr + deletedRecLen;
    // The source will be the current usedPtr location
    char *source = data + usedPtr;
    // The number of bytes to move by
    size_t numBytes = offset - usedPtr;
    // Perform the memory move
    memmove(destination, source, numBytes);


    // adjust where the usedPtr now points
    usedPtr = usedPtr + deletedRecLen;
    // since the record is deleted, add the available memory back to the freeSpace
    freeSpace = freeSpace + deletedRecLen;
    // adjust all the offset for slot[rid->slot] who is not empty
    // and its offset must be less than the original offset of the deleted record
    for (int i = 0; i <= slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT && slot[i].offset < offset) {
            slot[i].offset += deletedRecLen;
        }
    }
    // Make sure to grab the extra space back after deletion from the end of the slot array
    for (int i = slotCnt; i >= 0; i--) {
        // As soon as we hit an empty slot, we need to stop removing slots
        if (slot[i].length != EMPTY_SLOT)
            break;
        slotCnt = slotCnt - 1;
        freeSpace = freeSpace + sizeof(slot_t);
    }
    return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID &firstRid) {
    // Loop through the slots and find the first non-empty page
    for (int i = 0; i <= slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT) {
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
Status HFPage::nextRecord(RID curRid, RID &nextRid) {
    // Grab the current slot number
    int curNo = curRid.slotNo;
    // Make sure we're on the right page
    if (curRid.pageNo != curPage)
        return FAIL;
    // If we're under 0, it's invalid
    if (curNo < 0)
        return FAIL;
    // If we're over slot count, then just return done since no more records exist
    if (curNo > slotCnt)
        return DONE;
    // If the current slot is empty, we got an invalid slot
    if (slot[curNo].length == EMPTY_SLOT)
        return FAIL;

    // Find the next record and return ok
    for (int i = curNo + 1; i <= slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT) {
            nextRid.pageNo = curPage;
            nextRid.slotNo = i;
            return OK;
        }
    }
    // Return done if no more records can be found
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char *recPtr, int &recLen) {
    // Ensure it's the right page
    if (rid.pageNo != curPage)
        return FAIL;
    // Grab the slot, and make sure it's not empty
    int no = rid.slotNo;
    if (slot[no].length == EMPTY_SLOT)
        return FAIL;

    // Grab the slot offset
    int offset = slot[no].offset;
    // Grab the slot length
    recLen = slot[no].length;
    // Copy the memory into the record pointer
    memcpy(recPtr, &data[offset], recLen);
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char *&recPtr, int &recLen) {
    // Ensure it's the right page
    if (rid.pageNo != curPage)
        return FAIL;
    // Grab the slot, and make sure it's not empty
    int no = rid.slotNo;
    if (slot[no].length == EMPTY_SLOT)
        return FAIL;

    // Grab the slot offset
    int offset = slot[no].offset;
    // Grab the slot length
    recLen = slot[no].length;
    // Return the address of the memory instead of a copy
    recPtr = &data[offset];
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void) {
    // Just return the free space
    return freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void) {
    // Loop through until we hit an empty slot or hit the end of our slot directory
    for (int i = 0; i <= slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT)
            return false;
    }
    return true;
}