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
    // put your code here
    numBuffers = numbuf;
    bufPool = new Page[numbuf];
    bufDescr = new Descriptors[numbuf];
    for (int i = 0; i < numbuf; i++) {
        bufDescr[i].page_number = INVALID_PAGE;
    }
    hashTable = new unordered_map<int, int, IDHash>(HTSIZE);

}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr() {
    // put your code here
    Status status = flushAllPages();
    if (status != OK)
        return;
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
    // put your code here
    Status status;
    if (hashTable->find(PageId_in_a_DB) == hashTable->end()) {
        // loop through the entire buffer pool
        unsigned int index;
        for (index = 0; index < numBuffers && bufDescr[index].page_number != INVALID_PAGE; index++) {}

        // if index equals the number of buffers, it means that the buffer does not have any empty slots
        if (index == numBuffers) {
            int indexToReplace = -1;

            // Find the "most hated" page
            for (index = 0; index < numBuffers; index++) {
                if (bufDescr[index].love == 0 && bufDescr[index].hate > 0 && bufDescr[index].pin_count == 0) {
                    if (indexToReplace == -1) {
                        indexToReplace = index;
                    } else {
                        if (bufDescr[indexToReplace].hate > bufDescr[index].hate)
                            indexToReplace = index;
                    }
                }
            }

            // If no pages are hated, look through the loved pages
            if (indexToReplace == -1) {
                for (index = 0; index < numBuffers; index++) {
                    if (bufDescr[index].love > 0 && bufDescr[index].pin_count == 0) {
                        if (indexToReplace == -1) {
                            indexToReplace = index;
                        } else {
                            if (bufDescr[indexToReplace].love < bufDescr[index].love)
                                indexToReplace = index;
                        }
                    }
                }
            }

            if (indexToReplace != -1) {
                PageId oldPageId = bufDescr[indexToReplace].page_number;
                bufDescr[indexToReplace].page_number = PageId_in_a_DB;
                bufDescr[indexToReplace].pin_count = 1;
                bufDescr[indexToReplace].dirtybit = false;
                bufDescr[indexToReplace].love = 0;
                bufDescr[indexToReplace].hate = 0;
                status = MINIBASE_DB->write_page(oldPageId, &bufPool[indexToReplace]);
                if (status != OK)
                    return MINIBASE_CHAIN_ERROR(BUFMGR, status);
                if (emptyPage != TRUE) {
                    status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[indexToReplace]);
                    if (status != OK) return MINIBASE_CHAIN_ERROR(BUFMGR, status);
                }
                page = &bufPool[indexToReplace];
                hashTable->erase(oldPageId);
                hashTable->emplace(PageId_in_a_DB, indexToReplace);
                return OK;
            }

            return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERFULL);
        } else {
            // there is some space
            status = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[index]);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BUFMGR, status);
            bufDescr[index].page_number = PageId_in_a_DB;
            bufDescr[index].pin_count = 1;
            bufDescr[index].dirtybit = false;
            bufDescr[index].love = 0;
            bufDescr[index].hate = 0;
            page = &bufPool[index];
            hashTable->emplace(PageId_in_a_DB, index);
        }
    } else {
        // page exists so find where the frame number is
        int frameNumber = hashTable->at(PageId_in_a_DB);
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
    Descriptors *pageDescr = &bufDescr[frameNumber];
    if (dirty == true) {
        flushPage(page_num);
    }
    if (pageDescr->pin_count == 0)
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
    pageDescr->pin_count = pageDescr->pin_count - 1;

    if (hate == true)
    {
        pageDescr->hate++;
        for (int i = 0; i < numBuffers; i++)
            if (bufDescr[i].page_number != INVALID_PAGE)
                if (bufDescr[i].hate > 0) bufDescr[i].hate++;
    }
    else
    {
        pageDescr->love++;
        for (int i = 0; i < numBuffers; i++)
            if (bufDescr[i].page_number != INVALID_PAGE)
                if (bufDescr[i].love > 0) bufDescr[i].love++;
    }

    if (pageDescr->pin_count == 0) {
        // The page is ready to be replaced
    }
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
    if (bufDescr[frameNumber].pin_count != 0) {
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGEPINNED);
    }

    MINIBASE_DB->deallocate_page(globalPageId);
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