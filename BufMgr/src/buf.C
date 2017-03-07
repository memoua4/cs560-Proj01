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

BufMgr::BufMgr(int numbuf, Replacer *replacer)
{
    // put your code here
    numBuffers = numbuf;
    bufPool = new Page[numbuf];
    bufDescr = new Descriptors[numbuf];
    hashTable = new unordered_map<int, int, IDHash>(HTSIZE);

}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr()
{
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
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage)
{
    // put your code here
    return OK;
}//end pinPage

//*************************************************************
//** This is the implementation of unpinPage
// hate should be TRUE if the page is hated and FALSE otherwise
// if pincount>0, decrement it and if it becomes zero,
// put it in a group of replacement candidates.
// if pincount=0 before this call, return error.
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE)
{
    // Begin by grabbing the frame corresponding to the page
    // Make sure we're not trying to unpin a page that is not pinned
    if (hashTable->find(page_num) == hashTable->end())
    {
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTFOUND);
    }
    // Grab the frame number
    int frameNumber = hashTable->at(page_num);
    Descriptors *pageDescr = &bufDescr[frameNumber];
    if (dirty == true)
    {
        flushPage(page_num);
    }
    if (pageDescr->pin_count == 0)
        return MINIBASE_FIRST_ERROR(BUFMGR, BUFFERPAGENOTPINNED);
    pageDescr->pin_count = pageDescr->pin_count - 1;
    if (pageDescr->pin_count == 0)
    {
        // The page is ready to be replaced
    }
    return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany)
{
    // put your code here

    return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId)
{
    // Begin by grabbing the frame corresponding to the page
    // If it is found unpin the page first
    if (hashTable->find(globalPageId) != hashTable->end())
    {
        unpinPage(globalPageId);
    }
    MINIBASE_DB->deallocate_page(globalPageId);
    return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid)
{
    // put your code here
    return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages()
{
    for (unsigned int i = 0; i < numBuffers; i++)
        if (bufDescr[i].dirtybit == true)
            flushPage(bufDescr[i].page_number);

    return OK;
}


/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{
    //put your code here
    return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename)
{
    return unpinPage(globalPageId_in_a_DB, dirty);
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers()
{
    //put your code here
    return 0;
}