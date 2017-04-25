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

/*
 * Function: BTreeFileScan(BtreeFile *file, BTLeafPage *firstLeaf, const void *lo_key, const void* hi_key, AttrType keyType) 
 * @params 
 *          file: the btfile that is calling creating a new BTreeFileScan object 
 *          firstLeaf: the leaf of the page that contains the lowest key 
 *          lo_key: the lowest key 
 *          hi_key: the highest key 
 *          keyType: the key type of the page (attrString and attrInteger)
 *  @returns   
 *          void
 *   Description: The constructor uses the parameters to initializes the BTreeFile scanner. The constructor allows us to know 
 *                  the current page we want to start iterating through, the record to start, and stores the highest record.
 *                  1) After initializing the variables in the class, it iterate through the current leaf page until it either reaches the
 *                      the lowest key or the record who is one record larger than the lowest key. It gets the first record.
 */
BTreeFileScan::BTreeFileScan(BTreeFile *file, BTLeafPage *firstLeaf, const void *lo_key, const void* hi_key, AttrType keyType) {
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
    this->startKey = lo_key;
    this->endKey = hi_key;
    this->keyType = keyType;

    KeyType keyptr;
    RID dataRID;
    Status status;
    RID prevRID;
    if ( this->startKey != NULL ) {
        status = this->currentLeaf->get_first(this->currentLeafRID, &keyptr, dataRID);

        while ( status == OK && keyCompare(this->startKey, &keyptr, keyType) > 0 ) {
            prevRID = this->currentLeafRID;
            status = this->currentLeaf->get_next(this->currentLeafRID, &keyptr, dataRID);
        }

        if ( status == OK ) {
            this->currentLeafRID = prevRID;
        }
    }
}

/* 
 * Function: Deconstructor 
 * Description: Only unpins the page if the current leaf is not NULL. This means that there is an actual value high key 
 * for the value hi_key. If the hi_key is NULL, it iterates through all the leaf pages. The last page the scanner points  to 
 * is an invalid page. Therefore, the currentLeaf for such page should be NULL. We don't want to unpin the page twice, 
 * or we will get an error message from the Buffer Manager. 
 */
BTreeFileScan::~BTreeFileScan() {
    if (this->currentLeaf != NULL) {
        Status status = MINIBASE_BM->unpinPage(this->currentLeaf->page_no());
        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
        }
    }
}

/*
 * Function: get_next(RID &pageRID, void *keyptr)
 * @param:
 *              pageRID (passed by reference): this RID is modified and returned the value to the callee
 *              keyptr (passed by reference): this keyptr is modified and returns the next key to the callee
 * @return 
 *              OK: OK means there are still more records that needs to be scan and it was able to retrieve the next record
 *              DONE: DONE means that it finished scanning the entire scan, range scan, or equality scan
 * Description: It first checks if the the currentLeafRID already stores a current record. If it doesn't, it will grab the first record
 *              otherwise it gets the next record on the leaf page. If get_next() returns NOMORERECS, the function will get all the next
 *              possible pages until it finds a record. If the next page is INVALID_PAGE then it returns DONE. It means that it finished 
 *              scanning through all the leaf pages and its record. If a get_next() record is found, it does one last check. If hi_key is not
 *              NULL, it checks if record that was obtained had reached the highest search key 
 */
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

    if ( this->endKey != NULL && keyCompare(keyptr, this->endKey, this->keyType) > 0 ) {
        Status pinStatus = MINIBASE_BM->unpinPage(this->currentLeaf->page_no());
        if ( pinStatus != OK ) 
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        this->currentLeaf = NULL;
        this->currentLeafRID.pageNo = INVALID_PAGE;
        return DONE;
    }

    this->currentDeleted = false;
    pageRID = dataRID;
    return OK;
}

/*
 * Function: delete_current()
 * @return
 *          FAIL: if the currentLeaf is NULL
 *          DONE: if the current record is deleted already 
 *          OK: if the record is deleted from the page
 * Description: It deletes the current record being scanned from the page. The page must be an existing Leaf Page, 
 *              and the current record is not deleted, then it deletes the record from the page. 
 */
Status BTreeFileScan::delete_current() {
    if (this->currentLeaf == NULL)
        return FAIL;

    if (this->currentDeleted == true)
        return DONE;

    this->currentDeleted = true;
    Status status = this->currentLeaf->deleteRecord(this->currentLeafRID);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);
    
    return OK;
}


int BTreeFileScan::keysize() {
    return this->file->keysize();
}
