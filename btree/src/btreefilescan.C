/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "../include/minirel.h"
#include "../include/buf.h"
#include "../include/db.h"
#include "../include/new_error.h"
#include "../include/btfile.h"
#include "../include/btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan()
{
  // put your code here
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
  // put your code here
  return OK;
}

Status BTreeFileScan::delete_current()
{
  // put your code here
  return OK;
}


int BTreeFileScan::keysize() 
{
  // put your code here
  return OK;
}
