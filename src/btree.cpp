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

    } catch (FileNotFoundExceptio)  {  // This means file doesn't exist so create a file.

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
    //
    if (rootNode) {
        // call insert
    }

    // case 2 - traverse tree till you find a node to insert into
    // if not full - insert into tree
    // else call split node
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoNode
// this method is for when inserting into a leaf - checks where it fits
// if no space, finds where to split and calls split
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoNode(const int PageId, NonLeafNodeInt currNode, const void *key) {
    // insert end of the key array
    int size = currNode->keyArray.size;
    currNode->keyArray.add(*key);
    // for loop to traverse through the keyArray
    for (int i = 0; i < size; i++) {
        if (currNode->keyArray[size - i] < currNode->keyArray[size - i - 1])
            // swap key values
            int temp = currNode->keyArray[size - i - 1];
        currNode->keyArray[size - i - 1] = currNode->keyArray[size - i];
        currNode->keyArray[size - i] = temp;
        // swap rid values
        int tempRid = currNode->ridArray[size - i - 1];
        currNode->ridArray[size - i - 1] = currNode->keyArray[size - i];
        currNode->ridArray[size - i] = temp;
        // decrement the availSpace
        currNode->availSpace--;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoLeafNode
// this method is for when inserting into a leaf - checks where it fits and insert
// calls split if nodes is full
// calls the insertintoNode method to sort it
// -----------------------------------------------------------------------------
void BTreeIndex::insertIntoLeafNode(const RecordId rid, const PageId pid, const void *key, NonLeafNodeInt currNode) {
    Page *curPage;
    bufMgr->readPage(file, pid, curPage);

    LeafNodeInt *curNode = (LeafNodeInt *)curPage;
    // number of childNodes
    int numOfLeafNodes = curNode->pageNoArray.size;
    // read first node
    Page *insertPage;
    // look for a value less than final of a key array
    bool found = true;
    // keep looking till leaf node is selected to insert into
    while (found) {
        // for loop runs through all the children of a given Non Leaf Node
        for (int i = 0; i < numOfLeafNodes; i++) {
            // read the page
            bufMgr->readPage(this->file, curNode->pageNoArray[i], insertPage);
            // check if the key values is than the last value of the node
            if (*key < leafNode->keyArray[leafNode->keyArray.size - 1]) {
                // call insert - to insert into correct postion of page
                insertIntoNode(curNode->pageNoArray[i], currNocurNodede, *key) {
                    // break out of for loop
                    break;
                    // break out of while loop
                    found = false;
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::create new Root
// this method is for when to create a new Root node while propagating
// -----------------------------------------------------------------------------

void BTreeIndex::createNewRoot(const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild, bool aboveLeaf, int level) {
    Page *root;
    PageId rootId;
    // create non lead node for root
    NonLeafNodeInt *rootNode;
    bufMgr->allocPage(file, rootId, root);

    // initialize new root node
    rootNode = (NonLeafNodeInt *)root;
    // update new non leaf node
    if (aboveLeaf) {
        rootNode->level = 1;
    } else {
        rootNode->level = 0;
    }
    rootNode->keyArray[0] = *((int *)key);

    // add left child
    rootNode->pageNoArray[0] = leftChild;
    // add right child
    rootNode->pageNoArray[1] = rightChild;
    rootNode->spaceAvail--;

    // unpin page
    this->bufMgr->unPinPage(this->file, rootId, true);
    // do we need to update the meta?
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNodes -  None Leaf nodes
// called if spaceAvail = 0 when inserting
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNode(PageId pid, NonLeafNodeInt currNode, const void *key, const RecordId rid, const PageId leftChild, const PageId rightChild) {
    // reads the page to split
    Page *curPage;
    bufMgr->readPage(this->file, pid, currNode);
    // Create the new page(sibling)
    Page *newPage;
    PageId newPageId;
    // assign variables to sibling
    this->bufMgr->allocPage(this->file, newPageId, newPage);

    sibling->level = currNode->level;
    sibling->parentId = currNode->parentId;
    // insert the first two node values
    sibling->keyArray[0] = currNode->keyArray[0];
    sibling->keyArray[1] = currNode->keyArray[1];
    // assign the child page numbers to siblings
    sibling->pageNoArray[0] = currNode->pageNoArray[0];
    sibling->pageNoArray[1] = currNode->pageNoArray[1];
    // call insert into node
    insertIntoLeaf(currNode->parentPage, parent);
    // remove node?

    // assign children

    // reassign variables of the nodes
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNode -  Leaf nodes
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNode(const void *key, const RecordId rid) {
    // left biased
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
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() {
    if (!scanExecuting) {
        throw ScanNotInitializedException();
    }
    nextEntry = false;
    scanExecuting = false;
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = (PageId) -1;
    currentPageData = nullptr;
}

}  // namespace badgerdb
