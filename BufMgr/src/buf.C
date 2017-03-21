/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/


#include "../include/buf.h"


// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char *bufErrMsgs[] = {
        // error message strings go here
        "Not enough memory to allocate hash entry",
        "Inserting a duplicate entry in the hash table",
        "Removing a non-existing entry from the hash table",
        "Page not in hash table",
        "Not enough memory to allocate queue node",
        "Poping an empty queue",
        "OOOOOOPS, something is wrong",
        "Buffer pool full",
        "Not enough memory in buffer manager",
        "Page not in buffer pool",
        "Unpinning an unpinned page",
        "Freeing a pinned page"
};

// Create a static "error_string_table" object and register the error messages
// with minibase system 
static error_string_table bufTable(BUFMGR, bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr(int numbuf, Replacer *replacer) {
    // Initialize the fields of the class
    numBuffers = numbuf;
    bufPool = new Page[numbuf];
    bufDescr = new Descriptors[numbuf];
    // Ensure that each bufDescr is set to be an invalid page
    for (int i = 0; i < numbuf; i++) {
        bufDescr[i].page_number = INVALID_PAGE;
    }
    // Initialize our hash table
    hashTable = new unordered_map<int, int, IDHash>(HTSIZE);
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr() {
    // First flush all the pages in the buffer manager to disk
    flushAllPages();
    // Free the memory used by the buffer manager
    delete[] bufPool;
    delete[] bufDescr;
    delete hashTable;
}

//*************************************************************
//** This is the implementation of pinPage
    // Check if this page is in buffer pool, otherwise
    // find a frame for this page, read in and pin it.
    // also write out the old page if it's dirty before reading
    // if emptyPage==TRUE, then actually no read is done to bring
    // the page
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage) {
    Status status;
    // Check to see if the page is already in the hashTable and therefore has a frame
    if (hashTable->find(PageId_in_a_DB) == hashTable->end()) {
        // If it is not yet pinned in the buffer pool, find the correct slot to put the page in
        unsigned int index;
        // Check if any of the buffer pool's pages are open
        for (index = 0; index < numBuffers && bufDescr[index].page_number != INVALID_PAGE; index++) {}

        // if index equals the number of buffers, it means that the buffer does not have any empty slots
        if (index == numBuffers) {
            // This is the index of the page we will replace. It is determined by the love/hate replacement
            // policy
            int indexToReplace = -1;

            // Find the "most hated" page first, if there are no hated pages indexToReplace will = -1
            for (index = 0; index < numBuffers; index++) {
                // A page is replaceable if it has 0 love, and some amount of hate, and is not pinned.
                if (bufDescr[index].love == 0 && bufDescr[index].hate > 0 && bufDescr[index].pin_count == 0) {
                    // If we have a valid candidate, we either replace the current candidate if this page is
                    // more hated, or if we do not yet have a valid candidate then this is the one!
                    if (indexToReplace == -1) {
                        // Set the indexToReplace to be the current index
                        indexToReplace = index;
                    } else {
                        // If we already have a hated page, get the MOST HATED page and set that
                        // as the page to replace
                        if (bufDescr[indexToReplace].hate > bufDescr[index].hate)
                            indexToReplace = index;
                    }
                }
            }

            // If no pages are hated, look through the loved pages. Choose the least loved page
            if (indexToReplace == -1) {
                // Loop through each page
                for (index = 0; index < numBuffers; index++) {
                    // A page must be loved (love > 0), and not be pinned to be replaced
                    if (bufDescr[index].love > 0 && bufDescr[index].pin_count == 0) {
                        // Same as the hate policy, except we find the least loved page
                        if (indexToReplace == -1) {
                            // If we don't yet have a loved page, and we found one, just assign
                            // it to our index to replace
                            indexToReplace = index;
                        } else {
                            // If we found a page with less love than the current indexToReplace,
                            // then we replace it instead
                            if (bufDescr[indexToReplace].love < bufDescr[index].love)
                                indexToReplace = index;
                        }
                    }
                }
            }

            // If we found a page to replace, perform the replace
            if (indexToReplace != -1) {
                // Store the old page id off
                PageId oldPageId = bufDescr[indexToReplace].page_number;
                // Update the page id, pin count, dirtyBit, love, and hate
                bufDescr[indexToReplace].page_number = PageId_in_a_DB;
                bufDescr[indexToReplace].pin_count = 1;
                bufDescr[indexToReplace].dirtybit = false;
                bufDescr[indexToReplace].love = 0;
                bufDescr[indexToReplace].hate = 0;
                // Write the old page to disk
                status = MINIBASE_DB->write_page(oldPageId, &bufPool[indexToReplace]);
                // Make sure the write worked
                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BUFMGR, status);
                // If the page should not be empty, read it from the buffer pool, otherwise
                // just leave it blank
                if (emptyPage == FALSE) {
                    status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[indexToReplace]);
                    if (status != OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status);
                }
                // Point the page at the location of the buffer pool
                page = &bufPool[indexToReplace];
                // Erase the old page id from the hash table
                hashTable->erase(oldPageId);
                // Put the new page id with the frame number
                hashTable->emplace(PageId_in_a_DB, indexToReplace);

                // Everything went ok!
                return OK;
            }

            // Else, either all pages are pinned, or no pages were replaceable
            return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERFULL);
        } else {
            // there is some space left in the buffer pool so just add to the end
            status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[index]);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BUFMGR, status);
            // Initialize the bufDescr
            bufDescr[index].page_number = PageId_in_a_DB;
            bufDescr[index].pin_count = 1;
            bufDescr[index].dirtybit = false;
            bufDescr[index].love = 0;
            bufDescr[index].hate = 0;
            // Point the page at the location in the buffer pool
            page = &bufPool[index];
            // Put the key&frame number into the hashtable
            hashTable->emplace(PageId_in_a_DB, index);
        }
    } else {
        // page exists so find where the frame number is
        int frameNumber = hashTable->at(PageId_in_a_DB);
        // Point the page at the buffer pool page
        page = &bufPool[frameNumber];
        // add to its pin count
        bufDescr[frameNumber].pin_count++;
    }
    return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
// hate should be TRUE if the page is hated and FALSE otherwise
// if pincount>0, decrement it and if it becomes zero,
// put it in a group of replacement candidates.
// if pincount=0 before this call, return error.
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE) {
    // Begin by grabbing the frame corresponding to the page
    // Make sure we're not trying to unpin a page that is not pinned
    if (hashTable->find(page_num) == hashTable->end()) {
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
    }
    // Grab the frame number
    int frameNumber = hashTable->at(page_num);
    // Grab the page descriptor
    Descriptors *pageDescr = &bufDescr[frameNumber];
    // If the page is dirty, flush it
    if (dirty == true) {
        flushPage(page_num);
    }
    // Ensure that the page is pinned
    if (pageDescr->pin_count == 0)
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
    // Reduce the pin count
    pageDescr->pin_count = pageDescr->pin_count - 1;

    // If we hate the page, increment the page's hate
    if (hate == true) {
        pageDescr->hate++;
        // Then go through each page, and if it is hated, increment its hate. This ensures
        // the MRU policy be replacing pages that have the highest hate
        for (int i = 0; i < numBuffers; i++)
            if (bufDescr[i].page_number != INVALID_PAGE)
                if (bufDescr[i].hate > 0) bufDescr[i].hate++;
    } else {
        // If the page is loved, increment that page's love
        pageDescr->love++;
        // Go through each page, and if the page is loved, increment its love. This
        // ensures the LRU policy since we can find the least loved pages
        for (int i = 0; i < numBuffers; i++)
            if (bufDescr[i].page_number != INVALID_PAGE)
                if (bufDescr[i].love > 0) bufDescr[i].love++;
    }

    // We're done!
    return OK;
}

//*************************************************************
//** This is the implementation of newPage
    // call DB object to allocate a run of new pages and
    // find a frame in the buffer pool for the first page
    // and pin it. If buffer is full, ask DB to deallocate
    // all these pages and return error
//************************************************************
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany) {
    // put your code here
    // Tells the DBMS to allocate a new page 
    Status status = MINIBASE_DB->allocate_page(firstPageId, howmany);
    Status statusDeallocate;
    if (status != OK) { // if the DBMS did not allcoate page apporpriately, it shoudl return an error message
        return MINIBASE_CHAIN_ERROR(BUFMGR, status);
    }

    status = pinPage(firstPageId, firstpage, TRUE); // attempts to pin the page 

    // if the Buffer Manager fails to pin the page, the Buffer Manager calls the DMBS to deallocate the page 
    // no new existing pages exist anymore
    if (status != OK) { 
        statusDeallocate = MINIBASE_DB->deallocate_page(firstPageId, howmany);
        if (statusDeallocate != OK) {
            return MINIBASE_CHAIN_ERROR(BUFMGR, statusDeallocate);
        }
    }

    // returns OK if the new page is allocated, otherwise it returns a different result 
    return status;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId) {
    // Begin by grabbing the frame corresponding to the page
    int frameNumber = hashTable->at(globalPageId);
    // Make sure that the page is not pinned so we can free it
    if (bufDescr[frameNumber].pin_count != 0) {
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
    }

    // Attempt to deallocate the page
    Status status = MINIBASE_DB->deallocate_page(globalPageId);
    if (status != OK) {
        return MINIBASE_CHAIN_ERROR(BUFMGR, status);
    }

    return OK;
}

//*************************************************************
//** This is the implementation of flushPage
    // Used to flush a particular page of the buffer pool to disk
    // Should call the write_page method of the DB class
//************************************************************
Status BufMgr::flushPage(PageId pageid) {
    // first find if the page exists in the hash table 
    // if it doesn't exist, it returns an error message   
    if (hashTable->find(pageid) == hashTable->end()) {
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
    }

    // find the frame number of where the page is located in the buffer pool 
    int frameNumber = hashTable->at(pageid);

    // write the page on memory to disk - now memory and disk have same information 
    Status status = MINIBASE_DB->write_page(pageid, &bufPool[frameNumber]);
    if (status != OK) // if the DBMS had trouble writing the page, it should return an error message 
        return MINIBASE_CHAIN_ERROR(BUFMGR, status);

    bufDescr[frameNumber].dirtybit = false; // page is written to disk. The file on memory is the same as the file on disk. reset the dirty bit to false

    return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages() {
    // loops through the number of buffers and flush all the pages
    for (unsigned int i = 0; i < numBuffers; i++)
        if (bufDescr[i].dirtybit == true)
            flushPage(bufDescr[i].page_number);

    return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
    // Should be equivalent to the above pinPage()
    // Necessary for backward compatibility with project 1
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename) {
    return pinPage(PageId_in_a_DB, page, emptyPage);
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename) {
    return unpinPage(globalPageId_in_a_DB, dirty);
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
// Get number of unpinned buffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers() {
    //put your code here
    int numOfBuffers = 0;
    for (unsigned int i = 0; i < numBuffers; i++) {
        // we have to make sure that the page number is not invalid (because if it is invalid, no page has been pinned in that buffer)
        // the pin count of the page should be zero (indicating the page has been unpinned) 
        // if it fits all these criteria, it will increment the numOfBuffers (indicates how many pages are unpinned) 
        if ( bufDescr[i].page_number != INVALID_PAGE && bufDescr[i].pin_count == 0 )
            numOfBuffers++;
    }
    // return number unpinned buffers 
    return numOfBuffers;
}