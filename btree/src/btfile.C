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

static error_string_table btree_table( BTREE, BtreeErrorMsgs);

/*
	an index with given filename should already exist,
	this opens it.
*/
	 BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
	Status status;
	
	status = MINIBASE_DB->get_file_entry(filename, headerPageId);
	
	if ( status != OK ) {
		returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
		return;
	}
	
	status = MINIBASE_BM->pinPage(headerPageId, headerPage);
	
	if ( status != OK ) {
		returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
		return;
	}
	
	strcpy(fileName, filename);
	returnStatus = OK;
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
	Status status;
	
	if ( keytype != attrString && keytype != attrInteger ) {
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, ATTRNOTFOUND);
		return;
	}

	status = MINIBASE_DB->get_file_entry(filename, headerPageId);

	if ( status != OK ) {
		status = MINIBASE_BM->newPage(headerPageId, headerPage);
		
		if ( status != OK ) {
			returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
			return;
		}
		
		status = MINIBASE_DB->add_file_entry(filename, headerPageId);
		
		if ( status != OK ) {
			returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
			return;
		}
		
		((HFPage *)headerPage)->init(headerPageId);
		
		BTLeafPage root;
		
		status = MINIBASE_BM->newPage(headerPageInfo->pageId, (Page *&)root);
		
		if ( status != OK ) {
			returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
			return;
		}
	
		root->init(headerPageInfo->pageId);
		//((HFPage *)root)->setPrevPage(headerPageId);
		//((HFPage *)headerPage)->setNextPage(headerPageInfo->pageId);
		
		status = MINIBASE_BM->unpinPage(headerPageInfo->pageId, true);
		
		if ( status != OK ) {
			returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
			return;
		}
		
	} else {
		status = MINIBASE_BM->pinPage(headerPageId, headerPage);
		
		if ( status != OK ) {
			returnStatus = MINIBASE_CHAIN_ERROR(BTREE, status);
			return;
		}
	}
	
	headerPageInfo->type = keytype;
	headerPageInfo->keysize = keysize;
	strcpy(fileName, filename);
	returnStatus = OK;
}

BTreeFile::~BTreeFile ()
{
	Status status;
	
	status = MINIBASE_BM->unpinPage(headerPageId, true);
	
	if ( status != OK ) {
		MINIBASE_CHAIN_ERROR(BTREE, status);
		return;
	}
	
	delete [] fileName;
}

Status BTreeFile::destroyFile ()
{
	// put your code here
	BTLeafPage *page;
	void *key;
	Page * root;
	Status status;
	PageId pageId;

	status = MINIBASE_BM->pinPage(headerPageInfo->pageId, (Page *&)root);

	if ( status != OK ) {
		return MINIBASE_CHAIN_ERROR(BTREE, status);
	}
	
	status = getBTLeafPage(root, &page, &pageId, NULL);

	if ( status != OK ) {
		return status;
	}
	
	while ( pageId != INVALID_PAGE ) {
		status = MINIBASE_BM->pinPage(pageId, page);
		
		if ( status != OK ) {
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		}
		
		RID rid, dataRid;
		
		status = page->get_first(rid, key, dataRid);
		
		if ( status != OK ) {
			return MINIBASE_CHAIN_ERROR(BTREE, status);
		}
		
		while ( true ) {
			delete(key, dataRid);
			
			status = page->get_next(rid, key, dataRid);
			
			if ( status != OK ) {
				break;
			}
			
			// handles getting the next 
		}
	}
	
	// delete index page and header page 
	
	return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
  // put your code here
	BTLeafPage *page;
	void *key;
	Page * root;
	Status status;
	PageId pageId;

	status = MINIBASE_BM-pinPage(headerPageInfo->pageId, (Page *&)root);

	if ( status != OK ) {
		return MINIBASE_CHAIN_ERROR(BTREE, status);
	}
	
	status = getBTLeafPage(root, &page, &pageId, key);
	
	// return DONE if no more space and OK 
}

Status BTreeFile::Delete(const void *key, const RID rid) {
  // put your code here
  
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
	IndexFileScan * scan;
	Status status;
	HFPage *root;
	BTLeafPage *page;
	PageId pageId;
	RID *rid;
	RID *dataRid;
	
	status = MINIBASE_BM->pinPage(headerPageInfo->pageId, (Page *&)root);
	
	if ( status != OK ) {
		MINIBASE_CHAIN_ERROR(BTREE, status);
		return NULL;
	}
	
	status = getBTLeafPage(root, &page, &pageId, lo_key);
	
	if ( hi_key == NULL ) {
		while (pageId != INVALID_PAGE) {
			while (scan->get_next(rid, key) == OK);
			
			PageId id = ((HFPage *)page)->getNextPage();
			
			status = MINIBASE_BM->unpinPage(pageId, true);
			
			if ( status != OK ) {
				MINIBASE_CHAIN_ERROR(BTREE, status);
				return NULL;
			}
			
			status = MINIBASE_BM->pinPage(id, (Page &*)page);
			
			if ( status != OK ) {
				MINIBASE_CHAIN_ERROR(BTREE, status);
				return NULL;
			}
			
			pageId = id;
		}
	} else if ( lo_key == hi_key ) {
		bool found = false;
		
		while ( scan->get_next(rid, key) == OK ) {
			if ( key == hi_key && key == lo_key ) { 
				found = true;
				break;
			}
		}
		
		if ( found == false ) {
			MINIBASE_CHAIN_ERROR(BTREE, RECNOTFOUND);
		}
		
	} else {
		
		bool isHiKey = false;
		
		while ( pageId != INVALID_PAGE ) {
			while (scan->get_next(rid, key) == OK) {
				if ( keycompare(key, hi_key) >= 0 ) {
					isHiKey = true;
				}
			}
			
			if ( isHiKey == true ) {
				break;
			}
		}
	}
	
	status = MINIBASE_BM->unpinPage(pageId, true);
	
	if ( status != OK ) {
		MINIBASE_CHAIN_ERROR(BTREE, status);
		return NULL;
	}
	
	status = MINIBASE_BM->unpinPage(headerPageInfo->pageId, true);
	
	if ( status != OK ) {
		MINIBASE_CHAIN_ERROR(BTREE, status);
		return NULL;
	}
	
	return scan;
}

int keysize(){
	return headerPageInfo->keysize;
}

Status BTreeFile::getBTLeafPage(HFPage *root, BTLeafPage &*page, PageId &id, void * lo_key) {
	Status status;
	HFPage *currPage = root;
	PageId pageId;
	RID *rid;
	void *key;
	
	if ( currPage->get_type() == INDEX ) {
		
		if ( lo_key == NULL ) {
			((BTIndexPage *)currPage)->get_first(rid, key, pageId);
			
			status = MINIBASE_BM->pinPage(pageId, (Page &*)currPage);
			
			if ( status != OK ) {
				return MINIBASE_CHAIN_ERROR(BTREE, status);
			}
			
			while ( currPage->get_type() != LEAF ) {
				PageId *tmpId;
				
				((BTIndexPage *)currPage)->get_first(rid, key, tmpId);
				
				status = MINIBASE_BM->unpinPage(pageId, true);
				
				if ( status != OK ) {
					return MINIBASE_CHAIN_ERROR(BTREE, status);
				}
				
				pageId = tmpId;
				
				status = MINIBASE_BM->pinPage(pageId, (Page &*)currPage);
				
				if ( status != OK ) {
					return MINIBASE_CHAIN_ERROR(BTREE, status);
				}
				
			}
			
			page = currPage;
			id = pageId;

		} else {
			
			((BTIndexPage *)currPage)->get_first(rid, key, pageId);
			
			status = MINIBASE_BM->pinPage(pageId, (Page &*)currPage);
			
			if ( status != OK ) {
				return MINIBASE_CHAIN_ERROR(BTREE, status);
			}
				
			while ( currPage->get_type() != LEAF ) {
				bool found = false;
				
				while ( found == false ) {
					if ( keycompare(lo_key, key) >= 0 ) {
						found = true;
					} else {
						
						PageId *nextPageId;
						
						((BTIndexPage *)currPage)->get_next(rid, key, nextPageId);
						
						status = MINIBASE_BM->pinPage(nextPageId, (Page &*)currPage);
						
						if ( status != OK ) {
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						}
						
						if ( nextPageId == INVALID_PAGE ) {
							found = true;
							
							status = MINIBASE_BM->unpinPage(pageId, true);

							if ( status != OK ) {
								return MINIBASE_CHAIN_ERROR(BTREE, status);
							}
							
							continue;
						}
						
						status = MINIBASE_BM->unpinPage(pageId, true);
						
						if ( status != OK ) {
							return MINIBASE_CHAIN_ERROR(BTREE, status);
						}
						
						pageId = nextPageId;

					}
				}
				
				PageId tmpId;
				
				((BTIndexPage *)currPage)->get_first(rid, key, tmpId);
				
				status = MINIBASE_BM->unpinPage(pageId, true);
				
				if ( status != OK ) {
					return MINIBASE_CHAIN_ERROR(BTREE, status);
				}
				
				pageId = tmpId;
				
				status = MINIBASE_BM->pinPage(pageId, (Page &*)currPage);
				
				if ( status != OK ) {
					return MINIBASE_CHAIN_ERROR(BTREE, status);
				}
				
			}
			
			page = ((BTLeafPage *)currPage);
			id = pageId;
			
		}

	} else if ( currPage->get_type() == LEAF ) {
		page = ((BTLeafPage *)currPage);
		id = pageId; 
	}
	
	return OK;
}