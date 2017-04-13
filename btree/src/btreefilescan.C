/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "../include/btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::BTreeFileScan(BTreeFile *file, BTLeafPage *firstLeaf) {
    this->file = file;
    this->currentLeaf = firstLeaf;
    if (firstLeaf != NULL) {
        Status status = MINIBASE_BM->pinPage(this->currentLeaf->page_no(), (Page *&) this->currentLeaf);
        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
            this->currentLeaf = NULL;
        }
        this->currentLeafRID.pageNo = INVALID_PAGE;
    }
    this->currentDeleted = false;
}

BTreeFileScan::~BTreeFileScan() {
    if (this->currentLeaf != NULL) {
        Status status = MINIBASE_BM->unpinPage(this->currentLeaf->page_no());
        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
        }
    }
}


Status BTreeFileScan::get_next(RID &pageRID, void *keyptr) {
    if (currentLeaf == NULL)
        return MINIBASE_FIRST_ERROR(BTREE, FAIL);

    RID dataRID;
    Status status;

    // Need to grab the first RID from the leaf page
    if (this->currentLeafRID.pageNo == INVALID_PAGE) {
        status = this->currentLeaf->get_first(this->currentLeafRID, keyptr, dataRID);
    // Need to grab the next RID from the leaf page
    } else {
        status = this->currentLeaf->get_next(this->currentLeafRID, keyptr, dataRID);
    }

    // While we have no more records on the leaf page, traverse the linked list of pages
    while (status == NOMORERECS) {
        // Advance to the next page in the linked list
        PageId nextPageID = this->currentLeaf->getNextPage();

        // Unpin the last page
        Status pinStatus = MINIBASE_BM->unpinPage(this->currentLeaf->page_no());
        if (pinStatus != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, pinStatus);

        // Ensure that the next page is valid
        if (nextPageID == INVALID_PAGE) {
            this->currentLeaf = NULL;
            this->currentLeafRID.pageNo = INVALID_PAGE;
            return DONE;
        }

        // Pin the next page
        pinStatus = MINIBASE_BM->pinPage(nextPageID, (Page *&) currentLeaf);
        if (pinStatus != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, pinStatus);

        // Grab the first element of the new page
        status = this->currentLeaf->get_first(this->currentLeafRID, keyptr, dataRID);
    }

    this->currentDeleted = false;
    pageRID = dataRID;
    return OK;
}

Status BTreeFileScan::delete_current() {
    if (this->currentLeaf == NULL)
        return FAIL;

    if (this->currentDeleted == true)
        return DONE;

    this->currentDeleted = true;
    this->currentLeaf->deleteRecord(this->currentLeafRID);

    return OK;
}


int BTreeFileScan::keysize() {
    return this->file->keysize();
}
