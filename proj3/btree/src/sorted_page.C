/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <cstdlib>
#include <algorithm>
#include <cstring>
#include "../include/sorted_page.h"
#include "../include/btindex_page.h"
#include "../include/btleaf_page.h"

const char *SortedPage::Errors[SortedPage::NR_ERRORS] = {
        //OK,
        //Insert Record Failed (SortedPage::insertRecord),
        //Delete Record Failed (SortedPage::deleteRecord,
};

/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord(AttrType keyTypeIn,
                                char *recPtrIn,
                                int recLenIn,
                                RID &ridOut) {
    // Insert the record as usual
    Status status = HFPage::insertRecord(recPtrIn, recLenIn, ridOut);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SORTEDPAGE, status);

    if (keyTypeIn != attrInteger && keyTypeIn != attrString)
        return MINIBASE_FIRST_ERROR(SORTEDPAGE, ATTRNOTFOUND);

    // Does this sort work yet? Need to confirm...

    // Sort the data using a lambda function
    std::sort(slot, slot + this->slotCnt + 1, [&](const slot_t first, const slot_t second) -> bool {
        if (first.length == EMPTY_SLOT)
            return false;
        if (second.length == EMPTY_SLOT)
            return true;
        short firstOffset = first.offset;
        short secondOffset = second.offset;
        char *firstData = &this->data[firstOffset];
        char *secondData = &this->data[secondOffset];
        return keyCompare(firstData, secondData, keyTypeIn) < 0;
    });

    return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord(const RID &ridOut) {
    return HFPage::deleteRecord(ridOut);
}

int SortedPage::numberOfRecords() {
    // Go through each record and check if it's valid
    int numRecords = 0;
    for (short i = 0; i <= slotCnt; i++) {
        if (slot[i].length != EMPTY_SLOT)
            numRecords = numRecords + 1;
    }
    return numRecords;
}
