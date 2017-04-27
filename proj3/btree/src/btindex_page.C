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
    // cout << "Inserted in Insert Key " << endl;
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
    RID currentRID;
    PageId tempPageNo;
    KeyType key;

    Status status = get_first(currentRID, &key, tempPageNo);

    while ( keyCompare(key1, &key, key_type) >= 0 && status == OK ) {
        maxPageNo = tempPageNo;
        status = get_next(currentRID, &key, tempPageNo);
    }

    if ( maxPageNo == INVALID_PAGE && status == OK ) {
        pageNo = tempPageNo;
    } else {
        pageNo = maxPageNo;
    }

    return OK;
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

    get_key_data(key, &dataType, &entry, length, (NodeType) type);

    pageNo = dataType.pageNo;

    return OK;
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

    get_key_data(key, &dataType, &entry, length, (NodeType) type);

    pageNo = dataType.pageNo;

    return OK;
}
