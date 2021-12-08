/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName,
                       BufMgr *bufMgrIn,
                       const int attrByteOffset,
                       const Datatype attrType) {
    // initialize variables
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
    scanExecuting = false;
    bufMgr = bufMgrIn;
    leafOccupancy = INTARRAYLEAFSIZE;
    nodeOccupancy = INTARRAYNONLEAFSIZE;
    Page *metaPage;
    Page *rootPage;
    // check correct datatype
    if (attrType != INTEGER) {
        std::cout << "Incorrect data type.";
        return;
    }
    // Creating the index file name, taken from the project specification
    std ::ostringstream indexStr;
    indexStr << relationName << '.' << attrByteOffset;
    outIndexName = indexStr.str();
    // Since meta contains information about the first page, we have to get the header page
    // but also have to see if the file exists.
    try {
        file = new BlobFile(outIndexName, false);
        headerPageNum = file->getFirstPageNo();

        // After we get the first page, we use meta's info to compare with the given info to see if it matches.
        bufMgr->readPage(file, headerPageNum, metaPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        if (relationName != meta->relationName) throw BadIndexInfoException("Index doesn't exist.");
        if (attributeType != meta->attrType) throw BadIndexInfoException("Index doesn't exist.");
        if (attrByteOffset != meta->attrByteOffset) throw BadIndexInfoException("Index  doesn't exist.");

        // unpin headerPage because we are finished with it but didn't modify it.
        bufMgr->unPinPage(file, headerPageNum, false);

    } catch (FileNotFoundException) {  // This means file doesn't exist so create a file.

        file = new BlobFile(outIndexName, true);

        // create a meta page for the new index.
        bufMgr->allocPage(file, headerPageNum, metaPage);
        bufMgr->allocPage(file, rootPageNum, rootPage);
        IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
        strncpy(meta->relationName, relationName.c_str(), 20);
        meta->attrType = attrType;
        meta->rootPageNo = rootPageNum;

        // Create the root.
        bufMgr->allocPage(file, rootPageNum, rootPage);
        NonLeafNodeInt *root = (NonLeafNodeInt *)rootPage;

        FileScan FS(relationName, bufMgr);
        // get every tuple from the relation and load into the new file.
        RecordId rid;
        while (true) {  // get all rids that meet the predicate and break if the end of the file of relationName is reached.
            try {
                // get
                FS.scanNext(rid);
                insertEntry(FS.getRecord().c_str() + attrByteOffset, rid);
            } catch (EndOfFileException EOFE) {
                break;
            }
        }
        // finished with the meta page and root page and we did modify it.
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
    }
    // bufMgrIn->readPage()
    // if (relationName != meta)
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    // The destructor should end any scan, clear state variables, unpin pinned pages, and flush index file,
    // and deletes the file object.
    if (scanExecuting) {
        try {
            endScan();
        } catch (ScanNotInitializedException) {
            std::cout << "No scan has been initialized.";
        }
    }
    scanExecuting = false;
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
    //if rootNode exists ??? 
    //check size of RootNode if it is less than NODEINTARRAY size 
    if(rootNode->keyArray.size < INTARRAYNONLEAFSIZE){
        //create page to insert 
        Page *newPage;
        //should I make this a constant value? 
        int rootNumber = 2; 
        this -> bufMgr -> readPage(this->file, rootNumber, newPage);
    }
    //call create new node 
   

    //case 2 - traverse tree till you find a node to insert into  
        //if not full - insert into tree 
        //else call split node 

    


}


// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoLeaf
//this method is for when inserting into a leaf - checks where it fits 
//if no space, finds where to split and calls split 
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoLeaf(const int PageId, NonLeafNodeInt currNode){

        //if less than first one 
        
        //split on index 1 (end key)
        //unpin record 1 

        //if less than second one 
        //split on index 1 
        //unpin record 2

        //if less than 3rd,4th, or > 4th 
        //split on index 2 
        //unpin record 3

        //else 
        //insert after 4th 

        //returns the new order for leaf 
        //if it is greater than or equal too int array size 
        //call split method on that Node 

}
// -----------------------------------------------------------------------------
// BTreeIndex::create new Root
//this method is for when to create a new Root node while propograting  
// -----------------------------------------------------------------------------
void BTreeIndex::createNewRoot(const int PageId Page, const bool aboveNonLeaf,  const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild ){
    //root page should always be 2 
    PageId rootId = 2;
    //create non lead node for root 
    NonLeafNodeInt rootNode;
    bufMgr->readPage(this -> file,rootId,rootNode);
    //update/create new non leaf node 
    //level = 0 unless right above NonLeafNode 
    if (aboveNonLeaf){
        rootNode -> level = 1; 
    }
    else{
        rootNode -> level = 0; 
    }
    rootNode -> keyArray.add(*key);
    //add left child 
    rootNode -> pageNoArray[0] = leftChild; 
    //add right child 
    rootNode -> pageNoArray[1] =  rightChild; 
    rootNode -> spaceAvail--; 
    //do we need to update the meta? 

}
// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNodes -  None Leaf nodes   
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNode(const PageNo Page, NonLeafNodeInt currNode, const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild){
    PageNo pageId = Page; 
     //reads the page to split 
    this.bufMgr->rootNode.readPage(this -> file,pageId,currNode);
    //Reads the parent node 
    NonLeafNodeInt parent;
    //read the parent page 
    this.bufMgr->rootNode.readPage(this -> file,currNode->parentPage,parent);
    //call insert into node 
    insertIntoLeaf(currNode->parentPage, parent )
    //remove node? 

}   
// -----------------------------------------------------------------------------
// BTreeIndex::splitNode -  Leaf nodes   
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNode(const void *key, const RecordId rid){
    //left biased 
    
    

        

        
}


// -----------------------------------------------------------------------------
// BTreeIndex::searchNode
// -----------------------------------------------------------------------------
void BTreeIndex::searchNode(const void *key, PageId &pid, PageId currentId, PageId parent) {
    // Reads the content of currentId into curPage
    Page *curPage;
    bufMgr->readPage(file, currentId, curPage);

    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;

    
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void *lowValParm,
                           const Operator lowOpParm,
                           const void *highValParm,
                           const Operator highOpParm) {
    if ((lowOpParm != 'GT' && lowOpParm != 'GTE') ||
        (highOpParm != 'LT' && highOpParm != 'LTE')) {
        throw BadOpcodesException();
    }

    lowOp = lowOpParm;
    highOp = highOpParm;

    // Have to cast to int pointer first and then reference to get int value
    lowValInt = *(int *)lowValParm;
    highValInt = *(int *)highOpParm;

    // Make sure the parameter makes sense
    if (lowValInt > highValInt) throw BadScanrangeException();

    // only one scan at a time
    if (scanExecuting) endScan();

    /** ########### begin scan ########### */
    scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId &outRid) {
    
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
if (!scanExecuting) {
    throw ScanNotInitializedException();
}
scanExecuting = false;

}

}  // namespace badgerdb
