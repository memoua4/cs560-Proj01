/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "../include/minirel.h"
#include "../include/buf.h"
#include "../include/db.h"
#include "../include/new_error.h"
#include "../include/btfile.h"
#include "../include/btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
    // Possible error messages
    // _OK
    // CANT_FIND_HEADER
    // CANT_PIN_HEADER,
    // CANT_ALLOC_HEADER
    // CANT_ADD_FILE_ENTRY
    // CANT_UNPIN_HEADER
    // CANT_PIN_PAGE
    // CANT_UNPIN_PAGE
    // INVALID_SCAN
    // SORTED_PAGE_DELETE_CURRENT_FAILED
    // CANT_DELETE_FILE_ENTRY
    // CANT_FREE_PAGE,
    // CANT_DELETE_SUBTREE,
    // KEY_TOO_LONG
    // INSERT_FAILED
    // COULD_NOT_CREATE_ROOT
    // DELETE_DATAENTRY_FAILED
    // DATA_ENTRY_NOT_FOUND
    // CANT_GET_PAGE_NO
    // CANT_ALLOCATE_NEW_PAGE
    // CANT_SPLIT_LEAF_PAGE
    // CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table(BTREE, BtreeErrorMsgs);

/*
 * Function: printPage() : void 
 * Description: This is a debugger. Helps debug the program whenever there is a problem. 
 * It beings by retrieving the first leaf page (the left most leaf page) and start printing all
 * the leaf page. It then moves up the b-tree and print all the keys for all the BTIndex page. 
 */

void BTreeFile::printPage() {
	PageId pageId;
	Page *page;
	Status status = getStartingBTLeafPage(pageId);

	if ( status != OK ) 
		return; 

	int count = headerPageInfo->height;

	while ( count >= 0 ) {
		status = MINIBASE_BM->pinPage(pageId, page);

		if ( status != OK ) 
			return;

		if ( ((SortedPage*) page)->get_type() == LEAF ) {
			BTLeafPage * leafp = (BTLeafPage*)page;
			RID metaRid, dataRid;
			KeyType key;
                                cout << "Page ID == " << pageId << endl;;
			status = leafp->get_first(metaRid, &key, dataRid);
			if ( status == OK ) {
				do {
					cout << "\tPage/Slot: " << dataRid.pageNo << "/" << dataRid.slotNo << "Key:";
					switch (headerPageInfo->keyType) {
						case attrString: 	cout << key.charkey; break;
						case attrInteger: 	cout << key.intkey; break;
						default: break;
					}
					cout << endl;
				} while ( (status = leafp->get_next(metaRid, &key, dataRid)) == OK);
			}
			PageId tmpId = pageId;

			if ( ((HFPage*)page)->getNextPage() != INVALID_PAGE ) {
				pageId = ((HFPage*)page)->getNextPage();
			} else {
				count = count - 2;
				pageId = parentPages[count].indexPageId;
			}

			status = MINIBASE_BM->unpinPage(tmpId);

			if ( status != OK ) 
				return;// MINIBASE_CHAIN_ERROR(BTREE, status);
		} else if ( ((SortedPage*) page)->get_type() == INDEX ) {
			BTIndexPage * indexp = (BTIndexPage*)page;
			RID metaRid;
			PageId pg;
			KeyType key;

			status = indexp->get_first(metaRid, &key, pg);

			if ( status == OK ) {
				do {
					cout << "Page: " << pg << "Key: ";
					switch (headerPageInfo->keyType) {
						case attrString: cout << key.charkey; break;
						case attrInteger: cout << key.intkey; break;
						default: break;
					}
					cout << endl;
				} while ( indexp->get_next(metaRid, &key, pg) == OK );
			}

			PageId tmpId = pageId;

			if ( ((HFPage*)page)->getNextPage() != INVALID_PAGE ) {
				pageId = ((HFPage*)page)->getNextPage();
			} else {
				count--;
				pageId = parentPages[count].indexPageId;
			}
			status = MINIBASE_BM->unpinPage(tmpId);

			if ( status != OK ) 
				return;// MINIBASE_CHAIN_ERROR(BTREE, status);
		}

	}
}

 /*
  * an index with given filename should already exist,
  * this opens it.
  */
BTreeFile::BTreeFile(Status& returnStatus, const char *filename) {
    Status status; // Validation Variable

    status = MINIBASE_DB->get_file_entry(filename, headerPageId);

    if (status != OK) {
        returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
        return;
    }

    status = MINIBASE_BM->pinPage(headerPageId, (Page*&) headerPageInfo);

    if (status != OK) {
        returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
        return;
    }

    fileName = new char[strlen(filename) + 1];
    strcpy(fileName, filename);
    parentPages = new IndexPage[headerPageInfo->height];
    returnStatus = OK;

}

/*
 * Function: BTreeFile(Status &returnStatus, const char *filename)
 * Description: Given an index ( or filename ), the constructor attempts to open the file. If the file does not exists in the 
 * DB, then it creates a new file. If the file exists, then the opens the file, otherwise it creates a new file and stores all the 
 * necessary information about the btree. It stores the filename, header page number, the root page number, the key type,
 * and the key size. There is also a struct that stores all predecessors (parent and grandparent) page number. The variable
 * is called parentPages. 
 */
BTreeFile::BTreeFile(Status& returnStatus, const char *filename,
        const AttrType keytype,
        const int keysize) {

    Status status;

    status = MINIBASE_DB->get_file_entry(filename, headerPageId);

    if (status != OK) {

        if (keytype != attrString && keytype != attrInteger) {
            returnStatus = MINIBASE_FIRST_ERROR(BTREE, ATTRNOTFOUND);
            return;
        }

        status = MINIBASE_BM->newPage(headerPageId, (Page*&) headerPageInfo);

        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

        ((HFPage*) headerPageInfo)->init(headerPageId);

        status = MINIBASE_DB->add_file_entry(filename, headerPageId);

        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

        BTLeafPage *rootPage;
        PageId rootPageId;

        status = MINIBASE_BM->newPage(rootPageId, (Page*&) rootPage);

        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

        rootPage->init(rootPageId);

        headerPageInfo->rootPageId = rootPageId;
        headerPageInfo->keyType = keytype;
        headerPageInfo->keySize = keysize;
        headerPageInfo->height = 1;

        status = MINIBASE_BM->unpinPage(rootPageId, true);

        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

    } else {

        status = MINIBASE_BM->pinPage(headerPageId, (Page*&) headerPageInfo);

        if (status != OK) {
            returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

    }

    fileName = new char[strlen(filename) + 1];
    strcpy(fileName, filename);
    parentPages = new IndexPage[headerPageInfo->height];
    for ( int i = 0; i < headerPageInfo->height; i++ ) 
        parentPages[i].indexPageId = 0;

    returnStatus = OK;
}

/* 
 * Function: ~BTreeFile()
 * Description: The deconstructor does not destroy the page. Instead, it unpins the header page id, but does not deallocate
 * the page. It closes the index, but will allow the index to open again when the running program calls the constructor again.
 */
BTreeFile::~BTreeFile() {
    Status status;

    if ( headerPageId != INVALID_PAGE ) {
        status = MINIBASE_BM->unpinPage(headerPageId, true);

        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
            return;
        }

        delete [] fileName;
        delete [] parentPages;
    }
}

/* 
 * Function: DestroyFile() : Status
 * Description: This function destroys the entire file.  It starts by obtaining the leaf most leaf page. The function deletes all the 
 * entries, unpins the page, and then free the page. It then proceeds to the next leaf page and deletes all the record and free
 * page. After it finishes freeing all the BTLeafPage, it then deletes all the parents and grandparents leaf. These are BTIndexPage.
 * After it finishes freeing all the BTIndexPage (all the pages in the tree, including the root tree), it unpins the file, frees the file,
 * and delete the entry from the DB. 
 */
Status BTreeFile::destroyFile() {
    Status status;

    PageId pageNo = headerPageInfo->rootPageId;
    status = getStartingBTLeafPage(pageNo, NULL);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    int count = headerPageInfo->height - 1;

    RID rid, dataRid, tmpRid;
    KeyType key;
    PageId nextPageId, pageId;
    Page* page;

    while ( count >= 0 ) {
        status = MINIBASE_BM->pinPage(pageNo, (Page*&)page);
        if ( status != OK ) 
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        /* Delete All the Records in the Page */
        if ( ((SortedPage*)page)->get_type() == LEAF ) {
            /* Get the first record on the page */
            status = ((BTLeafPage*)page)->get_first(rid, &key, dataRid);

            /* Delete all the remaining records on the page */
            while ( status == OK ) {
                Status deleteStatus = ((BTLeafPage*)page)->deleteRecord(rid);
                if ( deleteStatus != OK ) 
                    return MINIBASE_CHAIN_ERROR(BTREE, status);

                status = ((BTLeafPage*)page)->get_next(rid, &key, dataRid);
            }

            /* Get the next page */
            nextPageId = ((HFPage*)page)->getNextPage();

            /* Unpin the Page */
            status = MINIBASE_BM->unpinPage(pageNo);
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            status = MINIBASE_BM->freePage(pageNo);
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            if ( nextPageId == INVALID_PAGE ) {
                /* Go up the tree */
                count = count - 1;
                /* Stores the first parent's page number */
                if ( count >= 0 ) 
                    pageNo = parentPages[count].indexPageId;
            } else {
                /* Delete all the records on the next page */
                pageNo = nextPageId;
            }

        } else {

            /* Get first record on the Index Page */
            status = ((BTIndexPage*)page)->get_first(rid, &key, pageId);

            /* Delete all the remaining records on the page */
            while ( status == OK ) {
                /* Delete the Record */
                Status deleteStatus = ((BTIndexPage*)page)->deleteKey(&key, headerPageInfo->keyType, tmpRid);
                if ( deleteStatus != OK ) 
                    return MINIBASE_CHAIN_ERROR(BTREE, status);

                status = ((BTIndexPage*)page)->get_next(rid, &key, pageId);
            }

            /* Get the Next Page */
            nextPageId = ((HFPage*)page)->getNextPage();

            status = MINIBASE_BM->unpinPage(pageNo);
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);
            status = MINIBASE_BM->freePage(pageNo);
         
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            if ( nextPageId == INVALID_PAGE ) {
                count = count - 1;
                /* Get Parent Page */
                if ( count >= 0 ) 
                    pageNo = parentPages[count].indexPageId;
            } else {
                /* Delete all the records on the next page */
                pageNo = nextPageId;
            }
        }
    }

    status = MINIBASE_BM->unpinPage(headerPageId);
    if ( status != OK )
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = MINIBASE_BM->freePage(headerPageId);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = MINIBASE_DB->delete_file_entry(fileName);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    headerPageId = INVALID_PAGE;
    delete [] fileName;
    delete [] parentPages;

    return OK;
}

/* 
 * Function: insert(const void *key, const RID rid) 
 * @param: 
 *      key : the key being inserted
 *      rid : record id used to store the key 
 * @return:
 *      status : this indicates whether or not the record is inserted properly into the btree
 * Description: Given a key and rid, this method attempts to insert the record into the btree. It first checks to make sure that all record size
 * is smaller than the keysize. If it is larger than the actual keysize, the method returns an error message indicating the keysize of the key is 
 * bigger than the actual record key size. If it passes this check, it get the appropriate BTLeafPage that stores the record. It then checks if leaf
 * can still store the record. If the record's size is greater than the available space, then it splits the BTLeafPage. Otherwise, it will insert the 
 * record into the page. 
 */
Status BTreeFile::insert(const void *key, const RID rid) {
    Status status;
    int keyLength;
    int dataKeySize;
    int leafPageSize;
    int keySize = keysize();
    PageId leafPageId;
    RID dataRid;
    BTLeafPage* leafPage;

    keyLength = get_key_length(key, headerPageInfo->keyType);

    if (keyLength > keySize)
        return MINIBASE_FIRST_ERROR(BTREE, TUPLE_TOO_BIG);

    status = getStartingBTLeafPage(leafPageId, key);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = MINIBASE_BM->pinPage(leafPageId, (Page*&) leafPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    if ( leafPage->get_data_rid(key, headerPageInfo->keyType, dataRid) == OK ) {
        status = MINIBASE_BM->unpinPage(leafPageId);
        if ( status != OK ) 
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        return OK;
    }

    dataKeySize = get_key_data_length(key, headerPageInfo->keyType, LEAF);
    leafPageSize = ((HFPage*) leafPage)->available_space();

    if (dataKeySize > leafPageSize) {

        status = splitLeafPage(leafPage, leafPageId, (headerPageInfo->height - 1), key, rid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
    } else {

        status = leafPage->insertRec(key, headerPageInfo->keyType, rid, dataRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
    }

    status = MINIBASE_BM->unpinPage(leafPageId);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);    

    delete [] parentPages;
    parentPages = new IndexPage[headerPageInfo->height];
    for ( int i = 0; i < headerPageInfo->height; i++ ) 
        parentPages[i].indexPageId = 0;

    return OK;
}

/*
 * Function: Delete (const void *key, const RID rid)
 * @param: 
 *          key : indicates the key being deleted 
 *          rid : the rid of the key being deleted 
 * @return: 
 *          status : Determines if the record has been deleted appropriately from the page. It returns either Buffer Manager error message,
 *                      RECNOTFOUND, or OK. Buffer Manager failed statuses because it failed to pin or unpin pages. RECNOTFOUND - the record
 *                      does not exists in the page and OK if the record is deleted from the page. 
 * Description: It first obtains the BTLeafPage where the record lies. It then uses get_data_rid() to check if the record exists in the page. 
 * If the page does not have the record, it will return RECNOTFOUND; otherwise it returns OK, indicating that the record exists in the page.
 * It attempts to delete the record if the file exists. If it fails to attempt to delete the file, it will then return the corresponding error message
 * from the deleteRecord message. If the record is deleted, it returns OK; otherwise RECNOTFOUND. 
 */
Status BTreeFile::Delete(const void *key, const RID rid) {
    Status status;
    PageId pageId;
    BTLeafPage* page;

    status = getStartingBTLeafPage(pageId, key);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = MINIBASE_BM->pinPage(pageId, (Page*&) page);

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    RID dataRid;

    if ( page->get_data_rid(key, headerPageInfo->keyType, dataRid) == OK ) {
    	status = ((SortedPage*)page)->deleteRecord(dataRid);
    	if ( status != OK ) {
    		return MINIBASE_CHAIN_ERROR(BTREE, status);
    	}
            
            status = MINIBASE_BM->unpinPage(pageId);
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            return OK;
    } 

    status = MINIBASE_BM->unpinPage(pageId, true);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    return RECNOTFOUND;
}

/* 
 * Function: new_scan(const void *lo_key, const void *hi_key) 
 * @params
 *          lo_key: this is the starting key 
 *          hi_key: this is the ending key 
 * @return
 *          return the scan
 * Description: There are different conditions for this function. 
 *         lo_key = NULL && hi_key = NULL 
 *              A full tree scan (start from the beginning of the leaf page to the end of the leaf page)
 *         lo_key != NULL && hi_key != NULL
 *              An equality search
 *         lo_key != NULL && hi_key = NULL
 *              Starts at the record and continues to scan until it reaches the hi_key
 *         lo_key = NULL && hi_key != NULL
 *              Starts at the lowest key and continues to scan until it reaches the end of the record 
 * Creates a new instance of BTreeFileScan and initializes all the values by passing in the lo_key, hi_key, the leaf page, key type, and 
 * the BTreeFile. 
 */
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
    Status status;
    PageId pageId;
    BTLeafPage *page;

    status = getStartingBTLeafPage(pageId, lo_key);

    if (status != OK) {
        MINIBASE_CHAIN_ERROR(BTREE, status);
        return NULL;
    }

    status = MINIBASE_BM->pinPage(pageId, (Page*&) page);

    if ( status != OK ) {
    	MINIBASE_CHAIN_ERROR(BTREE, status);
    	return NULL;
    }

    BTreeFileScan *scan= new BTreeFileScan(this, page, lo_key, hi_key, headerPageInfo->keyType);
    
    status = MINIBASE_BM->unpinPage(pageId);
    if ( status != OK ) {
        MINIBASE_CHAIN_ERROR(BTREE, status);
        return NULL;
    }

    return scan;
}

/* 
 * Function: keysize()
 * @return
 *      return the keysize of the page 
 * Description: A simply statement that returns the key size of the BTree file. 
 */
int BTreeFile::keysize() {
    return headerPageInfo->keySize;
}

/* 
 * Function: splitLeafPage(BTLeafPage* leafPage, PageId leafPageId, int height, const void *key, const RID rid)
 * @param
 *              leafPage : the leaf page that will be used to be splitted
 *              leafPageId : the leaf page id that is associated witht he leaf page being passed in
 *              height : where we are in the height of the tree
 *              key : the key that will be inserted 
 *              rid : the associated RID of the key 
 * @return
 *              status : return the status (indicating whether or not it was able to split the leaf page)
 * Description: 
 *              1) Creates the new leaf page 
 *              2) Iterate through the old leaf page until it reaches half of the record
 *              3) Iterate through the second half of the old leaf page, inserting the second half of the record into the new leaf page and deleting
 *                  the record in the old leaf page. 
 *              4) Inserts the key
 *              5) Insert the new leaf page 
 *                      a) If the old leaf page is a the root page, it creates a new index page (this is the new root page) 
 *                      b) Otherwise, it inserts it into the index page 
 *                              ii) Checks if there is enough space to insert into the index page 
 *                                         - If there is not enough space, then it splits the index page
 *                                         - If there is enough space, it inserts the leaf page into the index page
 */
Status BTreeFile::splitLeafPage(BTLeafPage* leafPage, PageId leafPageId,
        int height, const void* key, const RID rid) {
    Status status;
    BTLeafPage* newLeafPage;
    PageId newLeafPageId;

    status = MINIBASE_BM->newPage(newLeafPageId, (Page*&) newLeafPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    newLeafPage->init(newLeafPageId);
    int totalRec = ((SortedPage*) leafPage)->numberOfRecords();
    int halfRec;
    if (totalRec % 2 == 0)
        halfRec = totalRec / 2;
    else
        halfRec = (totalRec / 2) + 1;

    int count = 0;

    RID rid2;
    RID dataRid2;
    KeyType dataKey;

    status = leafPage->get_first(rid2, &dataKey, dataRid2);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);
    
    count++;

    for (; count < halfRec; count++)
        leafPage->get_next(rid2, &dataKey, dataRid2);

    RID tmpRid;

    KeyType firstKey = dataKey;

    status = newLeafPage->insertRec(&dataKey, headerPageInfo->keyType, dataRid2, tmpRid);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = ((HFPage*)leafPage)->deleteRecord(rid2);

    for (; count < totalRec; count++) {
        leafPage->get_next(rid2, &dataKey, dataRid2);
        newLeafPage->insertRec(&dataKey, headerPageInfo->keyType, dataRid2, tmpRid);
        ((SortedPage*) leafPage)->deleteRecord(rid2);
    }

    PageId leafPageNextPageId = ((HFPage*) leafPage)->getNextPage();
    ((HFPage*) newLeafPage)->setNextPage(leafPageNextPageId);
    ((HFPage*) newLeafPage)->setPrevPage(leafPageId);
    ((HFPage*) leafPage)->setNextPage(newLeafPageId);

    if (keyCompare(key, &firstKey, headerPageInfo->keyType) < 0) {
        status = leafPage->insertRec(key, headerPageInfo->keyType, rid, tmpRid);
    } else {
        status = newLeafPage->insertRec(key, headerPageInfo->keyType, rid, tmpRid);
    }

    if (parentPages[height].indexPageId == INVALID_PAGE) {
        BTIndexPage *newRootPage;
        PageId newRootPageId;

        status = MINIBASE_BM->newPage(newRootPageId, (Page*&) newRootPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        newRootPage->init(newRootPageId);

        RID recRid;
        RID recDataRid;
        KeyType recKey;

            status = leafPage->get_first(recRid, &recKey, recDataRid);

            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            status = newRootPage->insertKey(&recKey, headerPageInfo->keyType, ((SortedPage*)leafPage)->page_no(), tmpRid);

        status = newLeafPage->get_first(recRid, &recKey, recDataRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        status = newRootPage->insertKey(&recKey, headerPageInfo->keyType, ((SortedPage*) newLeafPage)->page_no(), tmpRid);

        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        status = MINIBASE_BM->unpinPage(newRootPageId, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        headerPageInfo->rootPageId = newRootPageId;
        headerPageInfo->height = headerPageInfo->height + 1;
    } else {
        height--;
        BTIndexPage *parentPage;
        PageId parentPageId = parentPages[height].indexPageId;
        
        status = MINIBASE_BM->pinPage(parentPageId, (Page*&) parentPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        RID recRid;
        RID recDataRid;
        KeyType recKey;

        status = newLeafPage->get_first(recRid, &recKey, recDataRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        int availableSpace = ((HFPage*) parentPage)->available_space();
        int space = get_key_data_length(key, headerPageInfo->keyType, INDEX);

        if (space > availableSpace) {

            status = splitIndexPage(parentPage, parentPageId, height - 1, &recKey);

            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

        } else {

            status = parentPage->insertKey(&recKey, headerPageInfo->keyType, ((SortedPage*) newLeafPage)->page_no(), recRid);

            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);

            RID curRid;
            KeyType curKey;
            PageId curPageNo;
            status = parentPage->get_first(curRid, &curKey, curPageNo);

            while ( curPageNo != leafPageId && status == OK ) 
                status = parentPage->get_next(curRid, &curKey, curPageNo);

            if ( curPageNo != INVALID_PAGE ) 
                status = parentPage->deleteKey(&curKey, headerPageInfo->keyType, tmpRid);
            

            status = leafPage->get_first(recRid, &recKey, recDataRid);
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);

             status = parentPage->insertKey(&recKey, headerPageInfo->keyType, ((SortedPage*) leafPage)->page_no(), recRid);

             if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);
            
            status = MINIBASE_BM->unpinPage(parentPageId, true); 
            if ( status != OK ) 
                return MINIBASE_CHAIN_ERROR(BTREE, status);      

        }

    }
    status = MINIBASE_BM->unpinPage(newLeafPageId, true);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);
    return OK;
}

/* 
 * Function: splitIndexPage(BTIndexPage *page, PageId, pageId, int height, void *recKey) 
 * @param
 *              page : the index page being splitted
 *              page id : the page number of the index page being splitted
 *              height: where we are in the tree (height-wise)
 *              recKey : the key of the index page being inserted 
 * @return
 *              status : indicates whether or not the index page is able to split without any error message. 
 * Description: 
 *              1) Creates a new index page 
 *              2) Iterate through the old index page until it reaches half of the record 
 *              3) Iterate through the second half of the index page, storing the second half of the record intot he new index page and deleting the 
 *                  records in the old index page 
 *              4) Insert the Key
 *              5) Insert the new index page
 *                          i) If the index page is the root page, it creates a new root page 
 *                          ii) otherwise, it attempts to insert it. If there is not enough space, it does a recursive call and call splitIndexPage, otherwise 
 *                              it attempts to insert it into the index page
 */
Status BTreeFile::splitIndexPage(BTIndexPage* page, PageId pageId, int height, void* recKey) {
    Status status;
    BTIndexPage* newIndexPage;
    PageId newIndexPageId;

    status = MINIBASE_BM->newPage(newIndexPageId, (Page*&) newIndexPage);

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    newIndexPage->init(newIndexPageId);

    RID tmpRid;
    PageId tmpPageId;
    KeyType tmpKey;

    int totalRec = ((SortedPage*) page)->numberOfRecords();
    int halfRec;

    if (totalRec % 2 == 0)
        halfRec = totalRec / 2;
    else
        halfRec = (totalRec / 2) + 1;

    int count = 0;

    status = ((BTIndexPage*) page)->get_first(tmpRid, &tmpKey, tmpPageId);

    RID firstRid = tmpRid;

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    count++;

    for (; count < halfRec; count++)
        ((BTIndexPage*) page)->get_next(tmpRid, &tmpKey, tmpPageId);

    RID tmpRid2;
    KeyType firstKey = tmpKey;
    PageId firstPageNo = tmpPageId;
    status = newIndexPage->insertKey(&tmpKey, headerPageInfo->keyType, tmpPageId, tmpRid2);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = ((BTIndexPage*)page)->deleteKey(&tmpKey, headerPageInfo->keyType, tmpRid);

    KeyType newIndexFirstKey = tmpKey;
    RID newIndexFirstRid = tmpRid;

    for (; count < totalRec; count++) {
        ((BTIndexPage*) page)->get_next(tmpRid, &tmpKey, tmpPageId);
        newIndexPage->insertKey(&tmpKey, headerPageInfo->keyType, tmpPageId, tmpRid2);
        ((BTIndexPage*) page)->deleteKey(&tmpKey, headerPageInfo->keyType, tmpRid);
    }

    if ( keyCompare(recKey, &firstKey, headerPageInfo->keyType) < 0 ) {
        /* Insert into old index page */
        ((BTIndexPage*)page)->insertKey(&firstKey, headerPageInfo->keyType, firstPageNo, tmpRid2);
    } else {
        /* Insert into the new index page */
        newIndexPage->insertKey(&firstKey, headerPageInfo->keyType, tmpPageId, tmpRid2);
    }

    if (height == INVALID_PAGE) {
        /* Create New Root Page */
        BTIndexPage *newRootPage;
        PageId newRootPageId;

        status = MINIBASE_BM->newPage(newRootPageId, (Page*&) newRootPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        newRootPage->init(newRootPageId);
        status = newRootPage->insertKey(&firstKey, headerPageInfo->keyType, ((SortedPage*) page)->page_no(), firstRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        status = newRootPage->insertKey(&newIndexFirstKey, headerPageInfo->keyType, ((SortedPage*) newIndexPage)->page_no(), newIndexFirstRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        status = MINIBASE_BM->unpinPage(newRootPageId, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        status = MINIBASE_BM->unpinPage(newIndexPageId, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

    } else {
        BTIndexPage *parentPage;
        PageId parentPageId = parentPages[height].indexPageId;
        status = MINIBASE_BM->pinPage(parentPageId, (Page*&) parentPage);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        RID recRid;
        PageId recPageId;
        KeyType recKey;

        status = newIndexPage->get_first(recRid, &recKey, recPageId);

        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        int availableSpace = ((HFPage*) parentPage)->available_space();
        int space = get_key_data_length(&recKey, headerPageInfo->keyType, INDEX);

        if (space > availableSpace) {
            // split Index Page
            status = splitIndexPage(parentPage, parentPageId, (height - 1), &recKey);
        } else {
            status = parentPage->insertKey(&recKey, headerPageInfo->keyType, recPageId, newIndexFirstRid);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
        }

        status = MINIBASE_BM->unpinPage(parentPageId, true);
        if ( status != OK ) 
            return MINIBASE_CHAIN_ERROR(BTREE, status);
    }

    return OK;
}

/*
 * Function: getStartingBTLeafPage(PageId &leafPageId, const void* key)
 * @param 
 *              leafPageId : passed in by reference variables. This variable will store the approrpiate leaf page id 
 *              key : key used to find the appropriate leaf page 
 * @return 
 *              status : determines if it is able to obtain the first leaf page 
 * Description: 
 *              If key is NULL, it gets the left most leaf page 
 *              If key is not NULL, it gets the page that will be used to store the leaf page 
 *              1) Starts at the root page 
 *              2) Continues to iterate through the tree until reaches a BTLeafPage
 *                      a) If key is NULL, continue to grab the first record until the page is a leaf page
 *                      b) If key is NOT NULL, use get_page_no() to get the approrpiate page until the page is a leaf page
 *             3) Returns the leafPageId found
 */
Status BTreeFile::getStartingBTLeafPage(PageId& leafPageId,
        const void* key) {
    Status status;
    PageId pageNo = headerPageInfo->rootPageId;
    Page* page;
    int index = 0;    
    RID tmpRid;
    KeyType tmpKey;
    PageId prevPageId;

    parentPages[index].indexPageId = INVALID_PAGE;

    status = MINIBASE_BM->pinPage(pageNo, page);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    while (((SortedPage*) page)->get_type() != LEAF && pageNo != INVALID_PAGE) {
        PageId tmpPageNo;
        if (key == NULL) {
            status = ((BTIndexPage*) page)->get_first(tmpRid, &tmpKey, tmpPageNo);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
        } else {
            status = ((BTIndexPage*) page)->get_page_no(key, headerPageInfo->keyType, tmpPageNo);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
       }

        parentPages[index].indexPageId = pageNo;
        index++;
        status = MINIBASE_BM->unpinPage(pageNo, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        prevPageId = pageNo;
        pageNo = tmpPageNo;

       if ( pageNo != INVALID_PAGE ) {
	        status = MINIBASE_BM->pinPage(pageNo, page);
	        if (status != OK)
	            return MINIBASE_CHAIN_ERROR(BTREE, status);
      }
    }

     if ( pageNo == INVALID_PAGE ) {
    	leafPageId = prevPageId;
    } else {
    	leafPageId = pageNo;

    	status = MINIBASE_BM->unpinPage(pageNo, true);
    	if ( status != OK ) 
    		return MINIBASE_CHAIN_ERROR(BTREE, status);
    }
    return OK;
}