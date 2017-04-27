/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "../include/btleaf_page.h"

const char *BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);

/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                             AttrType key_type,
                             RID dataRid,
                             RID &rid) {
    KeyDataEntry entry;
    int entryLength;
    DataType dataType;
    dataType.rid = dataRid;
    Status status;

    make_entry(&entry, key_type, key, (NodeType) type, dataType, &entryLength);

    //cout << "INSERT LEAF: " << entry.key.intkey << ", " << entry.data.rid.slotNo << " =/= " << dataRid.slotNo << ", " << entry.data.rid.pageNo << " =/= " << dataRid.pageNo << endl;
    //cout << sizeof(KeyDataEntry) << ", " << entryLength << endl;
    status = SortedPage::insertRecord(key_type, (char *) &entry, entryLength, rid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, status);

    return OK;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */
// Status BTLeafPage::get_first(RID &rid,
//                              void *key,
//                              RID &dataRid) {

Status BTLeafPage::get_data_rid(const void *key1,
                                AttrType key_type,
                                RID &dataRid) {

    RID currentRID;
    Status status;
    KeyType key2;
    int compare;

    status = get_first(currentRID, &key2, dataRid);
    if ( status != OK ) 
        return NOMORERECS;

    while ( status == OK && (compare = keyCompare(key1, &key2, key_type)) != 0  ) {
        status = get_next(currentRID, &key2, dataRid);
    }

    if ( compare == 0 ) {
        dataRid = currentRID;
        return OK;
    }

    return RECNOTFOUND;

}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first(RID &rid,
                             void *key,
                             RID &dataRid) {
    Status status;
    status = HFPage::firstRecord(rid);

    if (status != OK)
        return NOMORERECS;

    DataType dataType;
    KeyDataEntry entry;
    int length;

    status = getRecord(rid, (char *) &entry, length);

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    get_key_data(key, &dataType, &entry, length, (NodeType) type);

    dataRid = dataType.rid;

    return OK;
}

Status BTLeafPage::get_next(RID &rid, void *key, RID &dataRid) {
    Status status;

    status = HFPage::nextRecord(rid, rid);

    if (status != OK)
        return NOMORERECS;

    DataType dataType;
    KeyDataEntry entry;
    int length;

    status = this->getRecord(rid, (char *) &entry, length);

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);

    get_key_data(key, &dataType, &entry, length, (NodeType) type);

    dataRid = dataType.rid;

    return OK;
}
