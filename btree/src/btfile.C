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

BTreeFile::BTreeFile(Status& returnStatus, const char *filename,
        const AttrType keytype,
        const int keysize) {

    Status status;

    if (keytype != attrString && keytype != attrInteger) {
        returnStatus = MINIBASE_FIRST_ERROR(BTREE, ATTRNOTFOUND);
        return;
    }

    status = MINIBASE_DB->get_file_entry(filename, headerPageId);

    if (status != OK) {

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
    returnStatus = OK;
}

BTreeFile::~BTreeFile() {
    Status status;

    status = MINIBASE_BM->unpinPage(headerPageId, true);

    if (status != OK) {
        MINIBASE_CHAIN_ERROR(BTREE, status);
        return;
    }

    delete [] fileName;
    delete [] parentPages;
}

Status BTreeFile::destroyFile() {
    Status status;

    PageId pageNo = headerPageInfo->rootPageId;
    Page* page;

    //    status = MINIBASE_BM->pinPage(pageNo, page);
    status = getStartingBTLeafPage(pageNo, NULL);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    int count = headerPageInfo->height - 1;

    RID rid;
    KeyType key;
    RID dataRid;
    PageId id;

    while (count >= 0) {
        PageId firstPageID = pageNo;

        if (pageNo == INVALID_PAGE) {
            break;
        }

        status = MINIBASE_BM->pinPage(pageNo, page);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        if (((SortedPage*) page)->get_type() == LEAF) {
            while (pageNo != INVALID_PAGE) {

                if (pageNo != firstPageID) {
                    status = MINIBASE_BM->pinPage(pageNo, page);
                    if (status != OK)
                        return MINIBASE_CHAIN_ERROR(BTREE, status);
                }

                status = ((BTLeafPage*) page)->get_first(rid, &key, dataRid);

                if (status == OK) {
                    do {
                        RID tmpRid = rid;
                        ((BTLeafPage*) page)->deleteRecord(tmpRid);
                    } while (((BTLeafPage*) page)->get_next(rid, &key, dataRid) == OK);
                }

                PageId oldPageId = pageNo;

                pageNo = ((HFPage*) page)->getNextPage();

                status = MINIBASE_BM->unpinPage(oldPageId);

                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BTREE, status);

                status = MINIBASE_BM->freePage(oldPageId);

                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BTREE, status);
            }

            pageNo = parentPages[count].indexPageId;
        } else {

            while (pageNo != INVALID_PAGE) {

                if (pageNo != firstPageID) {
                    status = MINIBASE_BM->pinPage(pageNo, page);

                    if (status != OK)
                        return MINIBASE_CHAIN_ERROR(BTREE, status);
                }

                status = ((BTIndexPage*) page)->get_first(rid, &key, id);

                if (status == OK) {
                    do {
                        KeyType tmpKey = key;
                        RID tmpRid = rid;
                        ((BTIndexPage*) page)->deleteKey(&tmpKey, headerPageInfo->keyType, tmpRid);
                    } while (((BTIndexPage*) page)->get_next(rid, &key, id) == OK);
                }

                PageId oldPageId = pageNo;

                pageNo = ((HFPage*) page)->getNextPage();

                status = MINIBASE_BM->unpinPage(oldPageId);

                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BTREE, status);

                status = MINIBASE_BM->freePage(oldPageId);

                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BTREE, status);

            }
            count--;
            pageNo = parentPages[count].indexPageId;
        }
    }

    status = MINIBASE_BM->unpinPage(headerPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    status = MINIBASE_BM->freePage(headerPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    delete [] fileName;
    delete [] parentPages;

    return OK;
}

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

    dataKeySize = get_key_data_length(key, headerPageInfo->keyType, LEAF);
    leafPageSize = ((HFPage*) leafPage)->available_space();

    // if ( rid.slotNo == 1750 ) {
    // 	cout << "Page Id = " << leafPageId << endl;
    // }

//     if ( rid.pageNo % 10 == 0 ) 
// cout << "Leaf Page Id " << leafPageId << endl;
    // if ( rid.pageNo == 70 || rid.pageNo == 80 || rid.pageNo == 90 ) {
    // 	cout << "Insert" << leafPageId << endl;
    // 	// cout << "rid.pageNo = " << rid.pageNo << " and rid.slotNo = " << rid.slotNo << endl;
    // }

    if (dataKeySize > leafPageSize) {
        status = splitLeafPage(leafPage, leafPageId, (headerPageInfo->height - 1), key, rid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
    } else {
    	    if ( rid.slotNo == 150 ) {
    	cout << "Insert Page Id = " << leafPageId << endl;
    }
        status = leafPage->insertRec(key, headerPageInfo->keyType, rid, dataRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
    }

    //    for (int i = 0; i < headerPageInfo->height; i++) {
    //        cout << "Page Id is " << parentPages[i].indexPageId << endl;
    //    }

    delete [] parentPages;
    parentPages = new IndexPage[headerPageInfo->height];

    return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {
	// cout << endl << endl << endl;
	// cout << "-------------------------------Delete-------------------------------------------" << endl;
    Status status;
    PageId pageId;
    BTLeafPage* page;

    status = getStartingBTLeafPage(pageId, key);
// cout << "Page Id " << pageId << endl;
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
    	return OK;
    } else {
    	// cout << "Does not Exists ----------- " << endl;
    }

    return RECNOTFOUND;
	// return OK;
}

IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
    // put your code here

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

    BTreeFileScan *scan= new BTreeFileScan(this, page);

    scan->endKey = hi_key;

    return scan;
}

int BTreeFile::keysize() {
    return headerPageInfo->keySize;
}

Status BTreeFile::splitLeafPage(BTLeafPage* leafPage, PageId leafPageId,
        int height, const void* key, const RID rid) {
    //    cout << "Attempting to Split Now" << endl;
    Status status;
    BTLeafPage* newLeafPage;
    PageId newLeafPageId;

    status = MINIBASE_BM->newPage(newLeafPageId, (Page*&) newLeafPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    newLeafPage->init(newLeafPageId);
// cout << "New Leaf Page Id " << newLeafPageId << endl;
    //    cout << "Finished Creating a New Page" << endl;
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
    //    cout << "Got the First Record..." << endl;
    KeyType firstKey = dataKey;
    RID firstRid = rid2;

    count++;

    for (; count < halfRec; count++)
        leafPage->get_next(rid2, &dataKey, dataRid2);

    //    cout << "Got the Half Record..." << endl;
    RID tmpRid;

    status = newLeafPage->insertRec(&dataKey, headerPageInfo->keyType, dataRid2, tmpRid);
    if (status != OK)
        //        cout << "Failed to insert the first key" << endl;
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    //        cout << "Finished inserting the first key..." << endl;
    KeyType newLeafFirstKey = dataKey;
    RID newLeafFirstRid = rid2;

    for (; count < totalRec; count++) {
        leafPage->get_next(rid2, &dataKey, dataRid2);
        newLeafPage->insertRec(&dataKey, headerPageInfo->keyType, dataRid2, tmpRid);
        ((SortedPage*) leafPage)->deleteRecord(rid2);
    }
    //cout << "Finished Copying Records" << endl;
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

        //        newRootPage->insertKey(firstKey, )
        //        RID dummy;
        status = newRootPage->insertKey(&firstKey, headerPageInfo->keyType, ((SortedPage*) leafPage)->page_no(), firstRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        status = newRootPage->insertKey(&newLeafFirstKey, headerPageInfo->keyType, ((SortedPage*) newLeafPage)->page_no(), newLeafFirstRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        status = MINIBASE_BM->unpinPage(newRootPageId, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);

        status = MINIBASE_BM->unpinPage(newLeafPageId, true);
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
            return splitIndexPage(parentPage, parentPageId, height - 1, &recKey);
        } else {

            status = parentPage->insertKey(&recKey, headerPageInfo->keyType, ((SortedPage*) newLeafPage)->page_no(), recRid);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
        }

        status = MINIBASE_BM->unpinPage(parentPageId);    
    }

    return OK;
}

Status BTreeFile::splitIndexPage(BTIndexPage* page, PageId pageId, int height, void* recKey) {
    // cout << "Split Index Page" << endl;

    Status status;

    if (height == -1)
        return OK;

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

    KeyType firstKey = tmpKey;
    RID firstRid = tmpRid;

    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    count++;

    for (; count < halfRec; count++)
        ((BTIndexPage*) page)->get_next(tmpRid, &tmpKey, tmpPageId);

    RID tmpRid2;

    status = newIndexPage->insertKey(&tmpKey, headerPageInfo->keyType, tmpPageId, tmpRid2);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    KeyType newIndexFirstKey = tmpKey;
    RID newIndexFirstRid = tmpRid;

    for (; count < totalRec; count++) {
        ((BTIndexPage*) page)->get_next(tmpRid, &tmpKey, tmpPageId);
        newIndexPage->insertKey(&tmpKey, headerPageInfo->keyType, tmpPageId, tmpRid2);
        ((BTIndexPage*) page)->deleteKey(&tmpKey, headerPageInfo->keyType, tmpRid);
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

        status = MINIBASE_BM->unpinPage(parentPageId);
    }

    return OK;
}

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

    // cout << "Pin Page Number " << pageNo << endl;

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

        // cout << "Page No = " << pageNo << endl;

        status = MINIBASE_BM->unpinPage(pageNo);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
// cout << "Un Pin Page Number " << pageNo << endl;
        //        cout << "Page No2 is " << pageNo << endl;
            prevPageId = pageNo;
        pageNo = tmpPageNo;

        //        cout << "Page No is " << pageNo << endl;
       if ( pageNo != INVALID_PAGE ) {
	        status = MINIBASE_BM->pinPage(pageNo, page);
	        if (status != OK)
	            return MINIBASE_CHAIN_ERROR(BTREE, status);
	        // cout << "Pin Page Number " << pageNo << endl;
      }
    }

     if ( pageNo == INVALID_PAGE ) {
    	leafPageId = prevPageId;
    } else {
    	leafPageId = pageNo;

    	status = MINIBASE_BM->unpinPage(pageNo);
    	if ( status != OK ) 
    		return MINIBASE_CHAIN_ERROR(BTREE, status);
    // cout << "UnPin Page Number " << pageNo << endl;
    }
    return OK;
}