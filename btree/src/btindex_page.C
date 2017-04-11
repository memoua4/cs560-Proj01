/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "../include/btindex_page.h"

// Define your Error Messge here
const char *BTIndexErrorMsgs[] = {
        //Possbile error messages,
        //OK,
        //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

// ------------------- insertKey ------------------------
// Inserts a <key, page pointer> value into the index node.
// This is accomplished by a call to SortedPage::insertRecord()
// This function sets up the recPtr field for the call to
// SortedPage::insertRecord()
Status BTIndexPage::insertKey(const void *key,
                              AttrType key_type,
                              PageId pageNo,
                              RID &rid) {
    KeyDataEntry entry;
    NodeType nodeType = INDEX;
    DataType dataType;
    dataType.pageNo = pageNo;

    int entryLength = 0;
    make_entry(&entry, key_type, key, nodeType, dataType, &entryLength);
    Status status = SortedPage::insertRecord(key_type, (char *) &entry, entryLength, rid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    return OK;
}

Status BTIndexPage::deleteKey(const void *key, AttrType key_type, RID &curRid) {
    Status status;
    KeyDataEntry entry;
    void *currentToCompare = &entry;
    PageId currentPage;

    // Begin iterating over each of the keys

    status = get_first(curRid, currentToCompare, currentPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    while (keyCompare(key, currentToCompare, key_type) > 0)
    {
        status = get_next(curRid, currentToCompare, currentPage);
        if (status != OK)
            break;
    }

    // If we overshot the key we were looking for we gotta go back
    // a key due to the nature of the above loop
    if (keyCompare(key, currentToCompare, key_type) != 0)
        curRid.slotNo = curRid.slotNo - 1;

    status = SortedPage::deleteRecord(curRid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId &pageNo) {
    for (int i = slotCnt; i >= 0; i--)
    {
        if (slot[i].length != EMPTY_SLOT)
        {
            void *key2 = (data + slot[i].offset);
            int compare = keyCompare(key, key2, key_type);
            if (compare >= 0) // key1 >= key2
            {
                DataType* dataType = (DataType *) &pageNo;

                KeyDataEntry* entry = (KeyDataEntry *) key2;

                get_key_data(NULL, dataType, entry, slot[i].length, INDEX);

                return OK;
            }
        }
    }
    pageNo = this->getPrevPage();
    return OK;
}


Status BTIndexPage::get_first(RID &rid,
                              void *key,
                              PageId &pageNo) {
    if (slotCnt == 0)
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE, NOMORERECS);

    rid.pageNo = pageNo;
    rid.slotNo = -1; // Start at -1 so that when we increment in get_next we start at 0.

    return get_next(rid, key, pageNo);
}

Status BTIndexPage::get_next(RID &rid, void *key, PageId &pageNo) {
    int currentSlot = rid.slotNo + 1;

    if (currentSlot >= slotCnt)
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE, NOMORERECS);

    if (rid.pageNo != pageNo)
        return MINIBASE_FIRST_ERROR(BTINDEXPAGE, FAIL);

    DataType* dataType = (DataType *) &pageNo;

    KeyDataEntry* entry = (KeyDataEntry *) (data + slot[rid.slotNo].offset);

    get_key_data(key, dataType, entry, slot[rid.slotNo].length, INDEX);

    return OK;
}
