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

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID &dataRid) {
    int low = 0;
    int high = slotCnt;
    int middle;

    // Binary search while low <= high
    while (low <= high) {
        middle = (low + high) / 2;

        void *key2 = data + slot[middle].offset;
        int comparison = keyCompare(key, key2, key_type);
        if (comparison == 0) { // Keys match
            DataType *dataType = (DataType *) &dataRid;
            KeyDataEntry *entry = (KeyDataEntry *) (data + slot[middle].offset);
            get_key_data(NULL, dataType, entry, slot[middle].length, (NodeType) type);
            return OK;
        } else if (comparison > 0) { // key > key2
            high = middle - 1;
        } else if (comparison < 0) { // key < key2
            low = middle + 1;
        }
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
    if (this->numberOfRecords() == 0)
        return NOMORERECS;

    rid.pageNo = curPage;
    rid.slotNo = -1;

    return get_next(rid, key, dataRid);
}

Status BTLeafPage::get_next(RID &rid, void *key, RID &dataRid) {
    rid.slotNo = rid.slotNo + 1;

    if (rid.slotNo > slotCnt)
        return NOMORERECS;

    DataType* dataType = (DataType *) &dataRid;

    KeyDataEntry* entry = (KeyDataEntry *) (data + slot[rid.slotNo].offset);

    get_key_data(key, dataType, entry, slot[rid.slotNo].length, (NodeType) type);

    return OK;
}
