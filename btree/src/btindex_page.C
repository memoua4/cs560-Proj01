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
    DataType dataType;
    dataType.pageNo = pageNo;
    int entryLength;

    make_entry(&entry, key_type, key, (NodeType) type, dataType, &entryLength);
    //cout << "INSERT INDEX: " << entry.key.intkey << ", " << entry.data.pageNo << endl;
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

Status BTIndexPage::get_page_no(const void *key1,
                                AttrType key_type,
                                PageId &pageNo) {
    PageId maxPageNo = INVALID_PAGE;
    DataType maxDataType;

    RID currentRID;
    Status status = this->firstRecord(currentRID);
    while (status == OK)
    {
        int len;
        KeyDataEntry entry;
        this->getRecord(currentRID, (char *) &entry, len);
        int key2;
        DataType data;
        get_key_data(&key2, &data, &entry, len, (NodeType) type);
        if (keyCompare(key1, &key2, key_type) >= 0)
            if (key2 > maxPageNo) {
                maxPageNo = key2;
                maxDataType = data;
            }

        status = this->nextRecord(currentRID, currentRID);
    }

    if (maxPageNo != INVALID_PAGE)
        pageNo = maxDataType.pageNo;
    else
        pageNo = this->getPrevPage();

    return OK;

    /*
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

                get_key_data(NULL, dataType, entry, slot[i].length, (NodeType) type);

                return OK;
            }
        }
    }
    pageNo = this->getPrevPage();
    return OK;
     */
}


Status BTIndexPage::get_first(RID &rid,
                              void *key,
                              PageId &pageNo) {

    Status status;
    status = HFPage::firstRecord(rid);

    if ( status != OK ) 
        return NOMORERECS;

    DataType dataType;
    KeyDataEntry entry;
    int length;

    status = getRecord(rid, (char*)&entry, length);

    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    get_key_data(key, &dataType, &entry, length, INDEX);

    pageNo = dataType.pageNo;

    return OK;
    // if (this->numberOfRecords() == 0)
    //     return NOMORERECS;

    // rid.pageNo = curPage;
    // rid.slotNo = -1; // Start at -1 so that when we increment in get_next we start at 0.

    // return get_next(rid, key, pageNo);
}

Status BTIndexPage::get_next(RID &rid, void *key, PageId &pageNo) {
    Status status;

    status = HFPage::nextRecord(rid, rid);

    if ( status != OK ) 
        return NOMORERECS;

    DataType dataType;
    KeyDataEntry entry;
    int length;

    status = getRecord(rid, (char *)&entry, length);

    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    get_key_data(key, &dataType, &entry, length, INDEX);

    pageNo = dataType.pageNo;

    return OK;

    // rid.slotNo = rid.slotNo + 1;

    // if (rid.slotNo > slotCnt)
    //     return NOMORERECS;

    // DataType* dataType = (DataType *) &pageNo;

    // KeyDataEntry* entry = (KeyDataEntry *) (data + slot[rid.slotNo].offset);

    // get_key_data(key, dataType, entry, slot[rid.slotNo].length, (NodeType) type);

    return OK;
}
